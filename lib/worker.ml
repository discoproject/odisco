module JC = Json_conv
module P = Protocol
module N = Env
module E = Errors
module U = Utils

let expect_ok ic oc m =
  match P.send_request m ic oc with
    | P.M_ok -> ()
    | m -> raise (E.Worker_failure (E.Unexpected_msg (P.master_msg_name m)))

type output = {
  file : N.File.t;
  chan : out_channel;
  output : P.output;
}

let setup_task_env ic oc taskinfo =
  let out_files = ref ([] : (int option * output) list) in
  let out_channel ~label =
    try (List.assoc label !out_files).chan
    with Not_found ->
      (let file = N.open_output_file taskinfo label in
       let chan = Unix.out_channel_of_descr (N.File.fd file) in
       let output = { P.label = U.mapopt string_of_int label;
                      filename = N.File.name file;
                      otype = if label = None then P.Data else P.Labeled } in
       let out = { file; chan; output } in
       out_files := (label, out) :: !out_files;
       chan) in
  let task_rootpath = (Printf.sprintf "./.%s-%d-%f"
                         (P.string_of_stage taskinfo.P.task_stage)
                         taskinfo.P.task_id
                         (Unix.time ())) in
  let temp_dir = Filename.concat task_rootpath "tmp" in
  let interface_maker input_url input_path input_size = {
    Task.taskname = taskinfo.P.task_name;
    hostname = taskinfo.P.task_host;
    input_url;
    input_path;
    input_size;
    out_channel;
    log = (fun s -> expect_ok ic oc (P.W_message s));
    temp_dir;
  } in
  Unix.mkdir task_rootpath 0o766;
  Utils.init_logger task_rootpath;
  Unix.mkdir temp_dir 0o766;
  taskinfo.P.task_rootpath <- task_rootpath;
  out_files, interface_maker

let close_files out_files =
  flush_all ();
  let closer = fun (_, f) -> close_out f.chan; N.File.close f.file in
  List.iter closer out_files

let send_output_msg ic oc out_files =
  let sender = fun (_, f) -> expect_ok ic oc (P.W_output f.output) in
  List.iter sender out_files

let urls_of_replicas replicas =
  List.map snd replicas

let ids_of_replicas replicas =
  List.map fst replicas

let get_task_inputs ic oc excl =
  match P.send_request (P.W_input_exclude excl) ic oc with
    | P.M_task_input (status, inputs) -> status, inputs
    | m -> raise (E.Worker_failure (E.Unexpected_msg (P.master_msg_name m)))

type resolved_input =
  | Inp_replicas of P.input_id * (P.replica_id * Uri.t) list
  | Inp_splits of P.input_id * P.replica_id * Uri.t list

let id_of_resolved_input = function
  | Inp_replicas (id, _)
  | Inp_splits (id, _, _) -> id

(* FIXME: The below code doesn't handle input_status yet; this will be
   added once it's supported in the master. *)
let resolve_dirs taskinfo inputs label =
  let dirs, others = List.partition
    (fun (_id, _status, replicas) ->
      replicas <> []
      && P.scheme_of_uri (snd (List.hd replicas)) = P.Dir
    ) inputs in
  (* For each of the index inputs, fetch the index from one of the
     replicas. *)
  let make_req (id, _status, reps) =
    id, List.map (fun r -> P.norm_uri taskinfo (snd r)) reps in
  let indices = N.payloads_from taskinfo (List.map make_req dirs) in
  (* Map the fetched index back to a replica id *)
  let lookup_id id inps =
    let rec loop = function
      | [] -> assert false  (* The retriever should always return a valid id *)
      | ((iid, _, _) as inp) :: rest ->
        if iid = id then inp else loop rest
    in loop inps in
  let lookup_rid id uri inps =
    let _, _, reps = lookup_id id inps in
    let rec loop = function
      | [] ->
        (* FIXME: This could happen if we get HTTP redirects when fetching the index. *)
        assert false
      | (rid, u) :: rest ->
        if P.norm_uri taskinfo u = uri then rid else loop rest
    in loop reps in
  (* Process the retrieved indices *)
  let resolved_dirs = List.map
    (fun (id, result) ->
      match result with
        | U.Right (uri, index) ->
          let rid = lookup_rid id uri dirs in
          let parts = N.parse_index index in
          (match label with
            | Some l ->
              let selected = List.assoc l parts in
              let part_uri = P.norm_uri taskinfo (Uri.of_string selected) in
              U.Right (Inp_splits (id, rid, [part_uri]))
            | None ->
              let selected = List.map snd parts in
              let part_uris = List.map
                (fun p -> P.norm_uri taskinfo (Uri.of_string p))
                selected in
              U.Right (Inp_splits (id, rid, part_uris))
          )
        | U.Left e ->
          U.dbg "Error downloading input %d: %s" id (E.string_of_error e);
          U.Left (id, (match lookup_id id dirs with _, _, reps -> List.map fst reps), e)
    ) indices in
  (* Non-dir inputs are replica inputs *)
  resolved_dirs @ (List.map
                     (fun (id, _status, reps) -> U.Right (Inp_replicas (id, reps)))
                     others)

type localized_input =
  | Local_replica of P.input_id * Uri.t * N.File.t
  | Local_splits of P.input_id * (Uri.t * N.File.t) list

let download taskinfo resolved_inputs =
  let inputs = List.sort
    (fun i1 i2 -> compare (id_of_resolved_input i1) (id_of_resolved_input i2))
    resolved_inputs in
  let lookup_id id =
    let rec loop = function
      | [] -> assert false (* The retriever should always return a valid id *)
      | (Inp_replicas (iid, _) as inp) :: _ when iid = id -> inp
      | (Inp_splits (iid, _, _) as inp) :: _ when iid = id -> inp
      | _ :: rest -> loop rest in
    loop inputs in
  let make_reqs = function
    | Inp_replicas (id, reps) -> [ id, List.map (fun r -> P.norm_uri taskinfo (snd r)) reps ]
    | Inp_splits (id, _, splits) -> List.map (fun s -> id, [s]) splits in
  let downloads = N.inputs_from taskinfo (List.concat (List.map make_reqs inputs)) in
  (* Process in order of input id. *)
  let downloads = List.sort (fun (id1, _) (id2, _) -> compare id1 id2) downloads in
  let rec collect acc = function
    | [] -> acc
    | (id, dr) :: rest ->
      collect
        (if acc = [] then [id, [dr]]
         else if fst (List.hd acc) = id
         then (id, dr :: snd (List.hd acc)) :: List.tl acc
         else (id, [dr]) :: acc)
        rest in
  let downloads = collect [] downloads in
  assert (List.length downloads = List.length inputs);
  List.map (fun (id, dlist) ->
    match lookup_id id with
      | Inp_replicas (_, reps) ->
        (* We should retrieve only one of the replicas. *)
        assert (List.length dlist = 1);
        (match List.hd dlist with
          | U.Left e ->
            U.Left (id, List.map fst reps, e)
          | U.Right (uri, f) ->
            U.Right (Local_replica (id, uri, f)))
      | Inp_splits (id, rid, splits) ->
        (* We should retrieve all of the splits. *)
        assert (List.length dlist == List.length splits);
        let errors, results = U.lrsplit dlist in
        if errors <> [] then begin
          (* A partially downloaded split cannot be processed, and we
             report an error after closing any open files. *)
          U.dbg "Error downloading all %d splits from replica %d of input %d: %d errors"
            (List.length splits) rid id (List.length errors);
          List.iter (fun (_uri, f) -> N.File.close f) results;
          U.Left (id, [rid], List.hd errors)
        end else
          U.Right (Local_splits (id, results))
  ) downloads

let run_task ic oc taskinfo ?label task_init task_process task_done =
  let in_files = ref ([] : N.File.t list) in
  let out_files, intf_for_input = setup_task_env ic oc taskinfo in
  let callback = task_init (intf_for_input "" "" 0) in
  let process_download fi url =
    let fd = N.File.fd fi in
    let sz = (Unix.fstat fd).Unix.st_size in
      U.dbg "Input file name %s: length %d" (N.File.name fi) sz;
      assert (Unix.lseek fd 0 Unix.SEEK_SET = 0);
      task_process callback (intf_for_input (Uri.to_string url) (N.File.name fi) sz) (Unix.in_channel_of_descr fd);
      in_files := fi :: !in_files in
  let process_localized_input = function
    | Local_replica (id, uri, f) ->
      process_download f uri;
      id
    | Local_splits (id, inps) ->
      List.iter (fun (uri, f) -> process_download f uri) inps;
      id in
  let process inputs =
    (* First, do a parallel fetch of any partition indices in the
       inputs, and select partitions based on the label. *)
    let resolve_errors, resolved_inputs = U.lrsplit (resolve_dirs taskinfo inputs label) in
    let download_errors, downloaded_inputs = U.lrsplit (download taskinfo resolved_inputs) in
    let processed_ids = List.map process_localized_input downloaded_inputs in
    resolve_errors @ download_errors, processed_ids in
  let processed = ref [] in
  let fin = ref false in
    while (not !fin) do
      let status, inputs = get_task_inputs ic oc !processed in
      let errors, done_ = process inputs in
      processed := done_ @ !processed;
      (* Make one pass at processing retries for errors *)
      let retries = List.fold_left
        (fun acc (id, rids, e) ->
          match P.send_request (P.W_input_failure (id, rids)) ic oc with
            | P.M_fail ->
              U.dbg "Unable to get replacement inputs for failed input %d (failure: %s), bailing ..." id (E.string_of_error e);
              raise (E.Worker_failure e)
            | P.M_retry reps ->
              (id, (* unused *) P.Input_ok, reps) :: acc
            | m ->
              raise (E.Worker_failure (E.Unexpected_msg (P.master_msg_name m)))
        ) [] errors in
      let errors, redone = process (List.rev retries) in
      processed := redone @ !processed;
      fin := status == P.Task_input_done && errors = []
    done;
  task_done callback (intf_for_input "" "" 0);
  List.iter N.File.close !in_files;
  close_files !out_files;
  send_output_msg ic oc !out_files

let run_map ic oc taskinfo task =
  let module Task = (val task : Task.TASK) in
  let task_init, task_process, task_done = Task.map_init, Task.map, Task.map_done in
  run_task ic oc taskinfo task_init task_process task_done

let run_reduce ic oc taskinfo task =
  let module Task = (val task : Task.TASK) in
  let task_init, task_process, task_done = Task.reduce_init, Task.reduce, Task.reduce_done in
  run_task ic oc taskinfo task_init task_process task_done

let run ic oc taskinfo task =
  match taskinfo.P.task_stage with
    | P.Map -> run_map ic oc taskinfo task
    | P.Reduce -> run_reduce ic oc taskinfo task

let get_taskinfo = function
  | P.M_taskinfo ti -> ti
  | m -> raise (E.Worker_failure (E.Unexpected_msg (P.master_msg_name m)))

let start_protocol ic oc task =
  expect_ok ic oc (P.W_worker (P.protocol_version, Unix.getpid ()));
  let taskinfo = get_taskinfo (P.send_request P.W_taskinfo ic oc) in
  run ic oc taskinfo task

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
      | JC.Json_conv_error e ->
        err ((Printf.sprintf "Protocol parse error: %s\n" (JC.string_of_error e))
             ^ (Printf.sprintf "%s\n" (Printexc.get_backtrace ())))
      | e ->
        err ((Printf.sprintf "Uncaught exception: %s\n" (Printexc.to_string e))
             ^ (Printf.sprintf "%s\n" (Printexc.get_backtrace ())))

let start task =
  Printexc.record_backtrace true;
  error_wrap stdin stderr (fun () -> start_protocol stdin stderr task)
