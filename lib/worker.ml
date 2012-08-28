module JC = Json_conv
module P = Protocol
module N = Env
module E = Errors
module U = Utils
module L = Pipeline
module LU = Pipeline_utils

(** This module contains the core logic of interfacing between the
    Disco protocol and the task/user-code.
 *)

(* helpers for simple protocol interactions *)

let expect_ok ic oc m =
  match P.send_request m ic oc with
  | P.M_ok -> ()
  | m -> raise (E.Worker_failure (E.Unexpected_msg (P.master_msg_name m)))

let get_task_inputs ic oc excl =
  match P.send_request (P.W_input_exclude excl) ic oc with
  | P.M_task_input (status, inputs) -> status, inputs
  | m -> raise (E.Worker_failure (E.Unexpected_msg (P.master_msg_name m)))

(* task-generated outputs *)

type output = {
  file : N.File.t;
  chan : out_channel;
  output : P.output;
}

(** the task environment contains of

  (a) the per-task-run directory.  note that a task might run multiple
      times, even on the same machine, when the master deals with
      fault tolerance by re-running tasks.

  (b) the set of output files generated by this task

  (c) and the 'disco' argument given to the task's callbacks that
      allows the task to interface with its library environment
*)

let setup_task_env ic oc taskinfo =
  (* track output files *)
  let out_files = ref ([] : (L.label * output) list) in
  (* output file creator used by task *)
  let out_channel ~label =
    try (List.assoc label !out_files).chan
    with Not_found ->
      (let file = N.open_output_file taskinfo label in
       let chan = Unix.out_channel_of_descr (N.File.fd file) in
       let output = {P.label = label;
                     filename = N.File.name file} in
       let out = {file; chan; output} in
       out_files := (label, out) :: !out_files;
       chan) in
  (* task run directory *)
  let temp_dir = Filename.concat taskinfo.P.task_rootpath "tmp" in
  (* create the task's interface argument for a given input *)
  let interface_maker input_label input_url input_path input_size = {
    Task.jobname = taskinfo.P.task_jobname;
    jobfile = taskinfo.P.task_jobfile;
    stage = taskinfo.P.task_stage;
    grouping = taskinfo.P.task_grouping;
    group_label = taskinfo.P.task_group_label;
    group_node = taskinfo.P.task_group_node;
    task_id = taskinfo.P.task_id;
    hostname = taskinfo.P.task_host;
    input_label;
    input_url;
    input_path;
    input_size;
    out_channel;
    log = (fun s -> expect_ok ic oc (P.W_message s));
    temp_dir;
  } in
  Unix.mkdir taskinfo.P.task_rootpath 0o766;
  Utils.init_logfile taskinfo.P.task_rootpath;
  Unix.mkdir temp_dir 0o766;
  out_files, interface_maker

(* utilities to handle task outputs *)

let close_files out_files =
  flush_all ();
  List.iter (fun (_, f) -> close_out f.chan; N.File.close f.file
            ) out_files

let send_output_msg ic oc out_files =
  List.iter
    (fun (_, f) ->
      expect_ok ic oc (P.W_output (f.output, (N.File.size f.file)))
    ) out_files

(* Each task input is resolved either into: a labelled set of replicas
   from a "data" task input, or a labelled set of splits from a "dir"
   task input, where each split corresponds to an entry from the "dir"
   index. *)

type resolved_input =
  | Inp_replicas of P.input_id * L.label * (P.replica_id * Uri.t) list
  | Inp_splits of P.input_id * P.replica_id * (L.label * Uri.t) list

let id_of_resolved_input = function
  | Inp_replicas (id, _, _)
  | Inp_splits (id, _, _) -> id

(* The first pass of input processing resolves any Dir or Dir_indexed
   inputs, by fetching the specified index files and processing the
   entries as specified. *)

let parse_index s =
  let parse_line l =
    match U.string_split l ' ' with
    | l :: url :: _size :: [] -> (int_of_string l), (Uri.of_string url)
    | _ -> assert false
  in
  try Some (List.map parse_line (U.string_split s '\n'))
  with _ -> None

let get_labels entries label =
  List.fold_left (fun acc (l, u) -> if l = label then (l, u) :: acc else acc
                 ) [] entries

let resolve taskinfo inputs =
  (* Partition the inputs into dir indices and replicated data inputs. *)
  let dirs, ireps = List.fold_left
      (fun (dirs, ireps) ((id, _status, replicas) as inp) ->
        match replicas with
        | [] ->
            dirs, ireps
        | (_, (L.Dir _)) :: _
        | (_, (L.Dir_indexed _)) :: _ ->
            (inp :: dirs), ireps
        | (_, (L.Data (l, _))) :: _ ->
            (* Data inputs are trivially resolvable *)
            let reps = List.map (fun (rid, d) -> rid, L.uri_of d) replicas in
            dirs, (U.Right (Inp_replicas (id, l, reps))) :: ireps
      ) ([], []) inputs in
  (* For each of the dir index inputs, fetch the index. *)
  let make_req (id, _status, reps) =
    let map, uris = List.split
        (List.map
           (fun (rid, dr) -> let u = L.uri_of dr in (u, rid), u)
           reps) in
    let dr_rep = snd (List.hd reps) in
    id, (dr_rep, map), uris in
  let ndirs = List.length dirs in
  let indices =
    if ndirs > 0 then begin
      U.dbg "Fetching %d indices ..." ndirs;
      let indices = N.payloads_from taskinfo (List.map make_req dirs) in
      U.dbg "... indices fetched.";
      indices
    end else [] in
  (* Process the retrieved indices *)
  let resolved_dirs = List.map
      (fun (id, (dr_rep, map), result) ->
        (* We're assuming in the assocs below that Disco internal
           index urls don't get changed, e.g. via redirects. *)
        match dr_rep, result with
        | L.Dir _, U.Right (uri, index) ->
            let rid = List.assoc uri map in
            (match parse_index index with
             | None ->
                 U.dbg "Error parsing index %d: %s" id (Uri.to_string uri);
                 U.Left (id, [rid], E.Invalid_dir_index (id, rid, uri))
             | Some entries ->
                 (* Use all entries in Dir inputs *)
                 U.Right (Inp_splits (id, rid, entries)))
        | L.Dir_indexed (l, _), U.Right (uri, index) ->
            let rid = List.assoc uri map in
            (match parse_index index with
             | None ->
                 U.dbg "Error parsing index %d: %s" id (Uri.to_string uri);
                 U.Left (id, [rid], E.Invalid_dir_index (id, rid, uri))
             | Some entries ->
                 (* Use the specified labels only for Dir_indexed inputs *)
                 U.Right (Inp_splits (id, rid, get_labels entries l)))
        | _, U.Left e ->
            U.dbg "Error downloading input %d: %s" id (E.string_of_error e);
            U.Left (id, List.map snd map, e)
        | L.Data _, _ ->
            assert false
      ) indices in
  (* Results are the directly resolvable inputs plus the inputs from
     resolved indices *)
  resolved_dirs @ ireps

(* The second phase of input processing downloads the urls resulting
   from the first phase into local files.  The downloading module
   handles the resolution of local urls into local files if possible.
 *)

type localized_input =
  | Local_replica of P.input_id * L.label * Uri.t * N.File.t
  | Local_splits of P.input_id * (L.label * Uri.t * N.File.t) list

let download taskinfo inputs =
  (* Download each input into a local file. *)
  let make_reqs = function
    | (Inp_replicas (id, label, reps)) as inp ->
        [ id, (label, inp), List.map snd reps ]
    | (Inp_splits (id, _rid, splits)) as inp ->
        List.map (fun (label, url) -> id, (label, inp), [url]
                 ) splits in
  let inp_reqs = List.concat (List.map make_reqs inputs) in
  let n_inp_reqs = List.length inp_reqs in
  let downloads =
    if n_inp_reqs > 0 then begin
      U.dbg "Fetching %d input files ..." n_inp_reqs;
      let downloads = N.inputs_from taskinfo inp_reqs in
      U.dbg "... input files fetched.";
      downloads
    end else [] in
  assert (List.length downloads = n_inp_reqs);
  let module SplitDownloads =
    Map.Make (struct type t = resolved_input let compare = compare end) in
  let add_split inp result map =
    try SplitDownloads.add inp (result :: (SplitDownloads.find inp map)) map
    with Not_found -> SplitDownloads.add inp [result] map in
  let rep_list, split_map = List.fold_left
    (fun (rep_list, split_map) -> function
      (* collect rids for failed downloads *)
      | _, (_, Inp_replicas (id, _, reps)), U.Left e ->
          U.Left (id, List.map fst reps, e) :: rep_list, split_map
      | _, (_, (Inp_splits (id, rid, _) as inp)), U.Left e ->
          rep_list, add_split inp (U.Left (id, [rid], e)) split_map
      | _, (_, Inp_replicas (id, label, _reps)), U.Right (uri, f) ->
          (* localize results for successful downloads *)
          U.Right (Local_replica (id, label, uri, f)) :: rep_list, split_map
      | _, (label, (Inp_splits (_, _, _splits) as inp)), U.Right (uri, f) ->
          (* collect splits for same input_id together *)
          rep_list, add_split inp (U.Right (label, uri, f)) split_map
    ) ([], SplitDownloads.empty) downloads in
  (* Collect local splits together *)
  SplitDownloads.fold
    (fun inp results acc ->
      match inp with
      | Inp_replicas _ ->
          assert false
      | Inp_splits (id, rid, splits) ->
          assert (List.length splits = List.length results);
          let errors, downloads = U.lrsplit results in
          if errors <> [] then begin
            (* A partially downloaded split cannot be processed, and
               we report an error after closing any open files. *)
            U.dbg "Error downloading all %d splits from replica %d of input %d: \
              %d errors"
              (List.length splits) rid id (List.length errors);
            List.iter (fun (_label, _uri, f) -> N.File.close f) downloads;
            U.Left (List.hd errors) :: acc
          end else
            U.Right (Local_splits (id, downloads)) :: acc
    ) split_map rep_list

let pipeline_inputs_of ti inputs =
  List.map (fun (id, st, replicas) -> id, st, LU.task_input_of ti id replicas
           ) inputs

let run_task ic oc taskinfo task_init task_process task_done =
  let in_files = ref ([] : N.File.t list) in
  let out_files, intf_for_input = setup_task_env ic oc taskinfo in
  let callback = task_init (intf_for_input 0 "" "" 0) in
  let process_download label url fi =
    let fd = N.File.fd fi in
    let sz = (Unix.fstat fd).Unix.st_size in
    U.dbg "Input file %s: length %d" (N.File.name fi) sz;
    assert (Unix.lseek fd 0 Unix.SEEK_SET = 0);
    task_process callback
      (intf_for_input label (Uri.to_string url) (N.File.name fi) sz)
      (Unix.in_channel_of_descr fd);
    in_files := fi :: !in_files in
  let process_localized_input = function
    | Local_replica (id, label, uri, f) ->
        process_download label uri f;
        id
    | Local_splits (id, inps) ->
        List.iter (fun (label, uri, f) -> process_download label uri f) inps;
        id in
  let process inputs =
    (* First, resolve any indexed inputs *)
    let r_errors, r_inputs = U.lrsplit (resolve taskinfo inputs) in
    (* Then, localize (e.g. download) the successfully resolved inputs *)
    let d_errors, d_inputs = U.lrsplit (download taskinfo r_inputs) in
    (* Finally, process the successfully localized inputs *)
    let processed_ids = List.map process_localized_input d_inputs in
    r_errors @ d_errors, processed_ids in
  let processed = ref [] in
  let fin = ref false in
  while (not !fin) do
    let status, inputs = get_task_inputs ic oc !processed in
    let errors, done_ = process (pipeline_inputs_of taskinfo inputs) in
    processed := done_ @ !processed;
    (* Make one pass at processing retries for errors *)
    let retries = List.fold_left
        (fun acc (id, rids, e) ->
          match P.send_request (P.W_input_failure (id, rids)) ic oc with
          | P.M_fail ->
              U.dbg "Unable to get replacement inputs for failed input %d \
                (failure: %s), bailing ..."
                id (E.string_of_error e);
              raise (E.Worker_failure e)
          | P.M_retry reps ->
              (id, (* unused *) P.Input_ok, reps) :: acc
          | m ->
              raise (E.Worker_failure (E.Unexpected_msg (P.master_msg_name m)))
        ) [] errors in
    let errors, redone =
      process (pipeline_inputs_of taskinfo (List.rev retries)) in
    processed := redone @ !processed;
    fin := status == P.Task_input_done && errors = []
  done;
  U.dbg "Done processing %d inputs, finalizing ..." (List.length !processed);
  task_done callback (intf_for_input 0 "" "" 0);
  List.iter N.File.close !in_files;
  close_files !out_files;
  send_output_msg ic oc !out_files

(* initialize the protocol and then proceed to the main executor *)

let start_protocol ic oc pipeline =
  expect_ok ic oc (P.W_worker (P.protocol_version, Unix.getpid ()));
  let taskinfo =
    match P.send_request P.W_taskinfo ic oc with
    | P.M_taskinfo ti -> ti
    | m -> raise (E.Worker_failure (E.Unexpected_msg (P.master_msg_name m))) in
  let stage = taskinfo.P.task_stage in
  let task =
    try List.assoc stage pipeline
    with Not_found -> raise (E.Worker_failure (E.Unknown_stage stage)) in
  let module Task = (val task : Task.TASK)
  in run_task ic oc taskinfo Task.task_init Task.task_process Task.task_done

(* error-trapping wrapper around the task executor *)

let error_wrap ic oc f =
  let err s =
    U.dbg "error: %s" s;
    expect_ok ic oc (P.W_fatal s)
  in try
    f ();
    expect_ok ic oc P.W_done
  with
  | E.Worker_failure e ->
      err ((Printf.sprintf "%s\n" (E.string_of_error e))
           ^ (Printf.sprintf "%s\n" (Printexc.get_backtrace ())))
  | L.Pipeline_error e ->
      err ((Printf.sprintf "%s\n" (L.string_of_pipeline_error e))
           ^ (Printf.sprintf "%s\n" (Printexc.get_backtrace ())))
  | JC.Json_conv_error e ->
      err ((Printf.sprintf "Protocol parse error: %s\n" (JC.string_of_error e))
           ^ (Printf.sprintf "%s\n" (Printexc.get_backtrace ())))
  | e ->
      err ((Printf.sprintf "Uncaught exception: %s\n" (Printexc.to_string e))
           ^ (Printf.sprintf "%s\n" (Printexc.get_backtrace ())))

(* The main entry point into the library, called directly from the
   task's main function. *)

let start pipeline =
  Printexc.record_backtrace true;
  error_wrap stdin stderr
    (fun () -> start_protocol stdin stderr pipeline)
