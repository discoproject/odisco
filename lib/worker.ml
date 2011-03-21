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
  let interface_maker input_url input_size = {
    Task.taskname = taskinfo.P.task_name;
    hostname = taskinfo.P.task_host;
    input_url;
    input_size;
    out_channel;
    log = fun s -> expect_ok ic oc (P.W_status s);
  } in
    Unix.mkdir task_rootpath 0o766;
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

let resolve_input settings taskinfo ?label (id, _status, input) =
  let norm_uri = (fun uri -> P.norm_uri settings uri) in
    match List.map Uri.of_string input with
      | [] ->
          []
      | (h :: _) as replicas ->
          (match P.scheme_of_uri h, label with
             | P.Dir, Some l ->
                 let index = N.read_index taskinfo (List.map norm_uri replicas) in
                 let selected = List.assoc l index in
                   [norm_uri (Uri.of_string selected)]
             | P.Dir, None ->
                 let index = N.read_index taskinfo (List.map norm_uri replicas) in
                 let selected = List.map snd index in
                   List.map (fun s -> norm_uri (Uri.of_string s)) selected
             | P.Disco, _ | P.Http, _ ->
                 List.map norm_uri replicas
             | s, _ ->
                 raise (E.Worker_failure (E.Unsupported_input_scheme (id, (P.string_of_scheme s))))
          )

let get_task_inputs ic oc excl =
  match P.send_request (P.W_input excl) ic oc with
    | P.M_inputs (status, inputs) -> status, inputs
    | m -> raise (E.Worker_failure (E.Unexpected_msg (P.master_msg_name m)))

let run_task ic oc settings taskinfo ?label task_init task_process task_done =
  let out_files, intf_for_input = setup_task_env ic oc taskinfo in
  let callback = task_init (intf_for_input "" 0) in
  let fail_on_input_error = false in
  let process_input (processed, failed) ((id, _st, _inp) as input) =
    match resolve_input settings taskinfo ?label input with
      | _ when List.mem id processed ->
          processed, failed
      | [] ->
          (id :: processed), failed
      | (h :: _) as replicas ->
          (match N.download replicas taskinfo with
             | N.Download_error err ->
                 expect_ok ic oc (P.W_input_failure (id, E.string_of_error err));
                 if fail_on_input_error then raise (E.Worker_failure err)
                 else processed, (id :: failed)
             | N.Download_file fi ->
                 let fd = N.File.fd fi in
                 let sz = (Unix.fstat fd).Unix.st_size in
                   U.dbg "Input file name %s: length %d" (N.File.name fi) sz;
                   assert (Unix.lseek fd 0 Unix.SEEK_SET = 0);
                   (* TODO: FIXME: h may not be the actual input url *)
                   task_process callback (intf_for_input (Uri.to_string h) sz) (Unix.in_channel_of_descr fd);
                   N.File.close fi;
                   (id :: processed), failed
          ) in
  let processed = ref [] in
  let fin = ref false in
    while (not !fin) do
      let status, inputs = get_task_inputs ic oc !processed in
      let newly_processed, failed =
        List.fold_left process_input (!processed, []) inputs in
        processed := newly_processed;
        fin := status == P.Inputs_done && failed = []
    done;
    task_done callback (intf_for_input "" 0);
    close_files !out_files;
    send_output_msg ic oc !out_files

let run_map ic oc settings taskinfo task =
  let module Task = (val task : Task.TASK) in
  let task_init, task_process, task_done = Task.map_init, Task.map, Task.map_done in
    run_task ic oc settings taskinfo task_init task_process task_done

let run_reduce ic oc settings taskinfo task =
  let module Task = (val task : Task.TASK) in
  let task_init, task_process, task_done = Task.reduce_init, Task.reduce, Task.reduce_done in
    run_task ic oc settings taskinfo task_init task_process task_done

let run ic oc settings taskinfo task =
  U.dbg "running task %s (%s %d)" taskinfo.P.task_name
    (P.string_of_stage taskinfo.P.task_stage) taskinfo.P.task_id;
  match taskinfo.P.task_stage with
    | P.Map -> run_map ic oc settings taskinfo task
    | P.Reduce -> run_reduce ic oc settings taskinfo task

let get_settings = function
  | P.M_settings set -> set
  | m -> raise (E.Worker_failure (E.Unexpected_msg (P.master_msg_name m)))

let get_taskinfo = function
  | P.M_taskinfo ti -> ti
  | m -> raise (E.Worker_failure (E.Unexpected_msg (P.master_msg_name m)))

let start_protocol ic oc task =
  expect_ok ic oc (P.W_version P.protocol_version);
  expect_ok ic oc (P.W_pid (Unix.getpid ()));
  let settings = get_settings (P.send_request P.W_settings ic oc) in
  let taskinfo = get_taskinfo (P.send_request P.W_taskinfo ic oc) in
    run ic oc settings taskinfo task

let error_wrap ic oc f =
  let err s =
    U.dbg "error: %s" s;
    expect_ok ic oc (P.W_error s)
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

let test_start task =
  Printexc.record_backtrace true;
  start task
