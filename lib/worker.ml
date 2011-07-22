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
    Unix.mkdir temp_dir 0o766;
    taskinfo.P.task_rootpath <- task_rootpath;
    out_files, interface_maker

let close_files out_files =
  flush_all ();
  List.iter (fun (_, f) ->
               close_out f.chan;
               N.File.close f.file;
            ) out_files

let send_output_msg ic oc out_files =
  List.iter (fun (_, f) ->
               expect_ok ic oc (P.W_output f.output)
            ) out_files

let urls_of_replicas replicas =
  List.map snd replicas

let ids_of_replicas replicas =
  List.map fst replicas

type resolved_input =
  | Inp_replicas of Uri.t list
  | Inp_splits of Uri.t list
  | Inp_error of E.error

let resolve_input taskinfo ?label (id, _status, replicas) =
  let norm_uri = (fun uri -> P.norm_uri taskinfo uri) in
    match List.map Uri.of_string (urls_of_replicas replicas) with
      | [] ->
          Inp_replicas []
      | (h :: _) as replica_urls ->
          (match P.scheme_of_uri h, label with
             | P.Dir, Some l ->
                 (* TODO: Currently, read_index->get_payload just directly
                    throws an Input_failure on error, which kills this task (but
                    not the job).  It would be more optimal to have it return
                    any error in retrieving the index and report it as a normal
                    input error, so that the master can re-schedule the
                    generating task. *)
                 let index = N.read_index taskinfo (List.map norm_uri replica_urls) in
                 let selected = List.assoc l index in
                   Inp_splits [norm_uri (Uri.of_string selected)]
             | P.Dir, None ->
                 let index = N.read_index taskinfo (List.map norm_uri replica_urls) in
                 let selected = List.map snd index in
                   Inp_splits (List.map (fun s -> norm_uri (Uri.of_string s)) selected)
             | P.Disco, _ | P.Http, _ ->
                 Inp_replicas (List.map norm_uri replica_urls)
             | s, _ ->
                 raise (E.Worker_failure (E.Unsupported_input_scheme (id, (P.string_of_scheme s))))
          )

let get_task_inputs ic oc excl =
  match P.send_request (P.W_input_exclude excl) ic oc with
    | P.M_task_input (status, inputs) -> status, inputs
    | m -> raise (E.Worker_failure (E.Unexpected_msg (P.master_msg_name m)))

let run_task ic oc taskinfo ?label task_init task_process task_done =
  let in_files = ref ([] : N.File.t list) in
  let out_files, intf_for_input = setup_task_env ic oc taskinfo in
  let callback = task_init (intf_for_input "" "" 0) in
  let fail_on_input_error = false in
  let process_download fi url =
    let fd = N.File.fd fi in
    let sz = (Unix.fstat fd).Unix.st_size in
      U.dbg "Input file name %s: length %d" (N.File.name fi) sz;
      assert (Unix.lseek fd 0 Unix.SEEK_SET = 0);
      task_process callback (intf_for_input (Uri.to_string url) (N.File.name fi) sz) (Unix.in_channel_of_descr fd);
      in_files := fi :: !in_files in
  let rec process_input (processed, failed) ((id, _st, replicas) as input) =
    if List.mem id processed then processed, failed
    else begin
      match resolve_input taskinfo ?label input with
        | Inp_splits [] | Inp_replicas [] ->
            (id :: processed), failed
        | Inp_replicas replica_urls ->
            (match N.download replica_urls taskinfo with
               | N.Download_error err ->
                   U.dbg "Download error on input %d: %s" id (E.string_of_error err);
                   (match P.send_request (P.W_input_failure (id, ids_of_replicas replicas)) ic oc with
                      | P.M_ok
                      | P.M_fail ->
                          if fail_on_input_error then raise (E.Worker_failure err)
                          else processed, (id :: failed)
                      | P.M_retry new_replicas ->
                          process_input (processed, failed) (id, _st, new_replicas)
                      | m -> raise (E.Worker_failure (E.Unexpected_msg (P.master_msg_name m)))
                   );
               | N.Download_file (fi, u) ->
                   process_download fi u;
                   (id :: processed), failed
            )
        | Inp_splits splits ->
            List.iter (fun split ->
                         match N.download [split] taskinfo with
                           | N.Download_error err ->
                               (* Since we may have partially consumed this
                                  input, we can't ask for a replacement; we can
                                  only throw an Input_failure and fail this
                                  task. *)
                               raise (E.Worker_failure err)
                           | N.Download_file (fi, _) ->
                               process_download fi split
                      ) splits;
            (id :: processed), failed
        | Inp_error err ->
            (match P.send_request (P.W_input_failure (id, ids_of_replicas replicas)) ic oc with
               | P.M_ok
               | P.M_fail ->
                   if fail_on_input_error then raise (E.Worker_failure err)
                   else processed, (id :: failed)
               | P.M_retry new_replicas ->
                   process_input (processed, failed) (id, _st, new_replicas)
               | m -> raise (E.Worker_failure (E.Unexpected_msg (P.master_msg_name m)))
            )
    end in
  let processed = ref [] in
  let fin = ref false in
    while (not !fin) do
      let status, inputs = get_task_inputs ic oc !processed in
      let newly_processed, failed =
        List.fold_left process_input (!processed, []) inputs in
        processed := newly_processed @ failed;
        fin := status == P.Task_input_done && failed = []
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
  U.dbg "running task %s (%s %d)" taskinfo.P.task_name
    (P.string_of_stage taskinfo.P.task_stage) taskinfo.P.task_id;
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
