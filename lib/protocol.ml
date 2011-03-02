module B = Dencode
module E = Errors
module U = Unix

let protocol_version = "0.1"

type stage =
  | Map
  | Reduce

let stage_of_string s =
  match String.lowercase s with
    | "map" -> Map
    | "reduce" -> Reduce
    | _ -> raise (E.Worker_failure (E.Unknown_stage s))

let string_of_stage = function
  | Map -> "map"
  | Reduce -> "reduce"

(* master -> worker *)

type settings = {
  settings_port : int;
  settings_put_port : int;
}

type taskinfo = {
  task_id : int;
  task_stage : stage;
  task_name : string;
  task_host : string;
  mutable task_rootpath : string;
}

type scheme =
  | Dir
  | Disco
  | File
  | Raw
  | Http
  | Other of string

let string_of_scheme = function
  | Dir -> "dir"
  | Disco -> "disco"
  | File -> "file"
  | Raw -> "raw"
  | Http -> "http"
  | Other s -> s

let scheme_of_uri uri =
  match uri.Uri.scheme with
    | None         -> File
    | Some "dir"   -> Dir
    | Some "disco" -> Disco
    | Some "file"  -> File
    | Some "raw"   -> Raw
    | Some "http"  -> Http
    | Some s       -> Other s

let norm_uri set uri =
  let trans_auth =
    match uri.Uri.authority with
      | None -> None
      | Some a -> Some { a with Uri.port = Some set.settings_port }
  in
    match uri.Uri.scheme with
      | None         -> { uri with Uri.scheme = Some "file" }
      | Some "dir"
      | Some "disco" -> { uri with Uri.scheme = Some "http";
                            authority = trans_auth }
      | Some _       -> uri

type inputs_status =
  | Inputs_more
  | Inputs_done

let inputs_status_of_string s =
  match String.lowercase s with
    | "more" -> Inputs_more
    | "done" -> Inputs_done
    | _      -> raise (E.Worker_failure (E.Unexpected_msg ("inputs ('" ^ s ^ "')")))

type input_status =
  | Input_ok
  | Input_failed

let input_status_of_string s =
  match String.lowercase s with
    | "ok"     -> Input_ok
    | "failed" -> Input_failed
    | _        -> raise (E.Worker_failure (E.Unexpected_msg ("inputs ('" ^ s ^ "')")))

type input_id = int

type input = input_id * input_status * string list

type master_msg =
  | M_ok
  | M_die
  | M_ignore
  | M_settings of settings
  | M_taskinfo of taskinfo
  | M_inputs of inputs_status * input list

let master_msg_name = function
  | M_ok -> "ok"
  | M_die -> "die"
  | M_ignore -> "ignore"
  | M_taskinfo _ -> "taskinfo"
  | M_settings _ -> "settings"
  | M_inputs _ -> "inputs"

let split s =
  let indx = (try Some (String.index s ' ') with _ -> None) in
    match indx with
      | None -> s, ""
      | Some i ->
          let slen = String.length s in
            (String.sub s 0 i), (String.sub s (i+1) (slen - i - 1))

let settings_of_bencode b =
  let lookup key = List.assoc key (B.dict_val b) in
  let settings_port = Int64.to_int (B.int_val (lookup "port")) in
  let settings_put_port = Int64.to_int (B.int_val (lookup "put_port")) in
    { settings_port; settings_put_port }

let taskinfo_of_bencode b =
  let lookup key = List.assoc key (B.dict_val b) in
  let task_id = Int64.to_int (B.int_val (lookup "taskid")) in
  let task_stage = stage_of_string (B.string_val (lookup "mode")) in
  let task_name = B.string_val (lookup "jobname") in
  let task_host = B.string_val (lookup "host") in
  let task_rootpath = "./" in
    { task_id; task_stage; task_name; task_host; task_rootpath }

let inputs_of_bencode b =
  let msg = B.list_val b in
  let inps_status = inputs_status_of_string (B.string_val (List.hd msg)) in
  let minps = B.list_val (List.hd (List.tl msg)) in
  let mk_inp =
    (fun l ->
       let l = B.list_val l in
       let inp_id = Int64.to_int (B.int_val (List.hd l)) in
       let inp_status = input_status_of_string (B.string_val (List.nth l 1)) in
       let inps = List.map B.string_val (B.list_val (List.nth l 2)) in
         inp_id, inp_status, inps) in
    inps_status, List.map mk_inp minps

let rec get_raw_master_msg ic =
  let inp = input_line ic in
  let msg, len_str = split inp in
    match msg, len_str with
      | "", "" ->
          get_raw_master_msg ic
      | _, "" | _, "0" ->
          msg, ""
      | _, len_str ->
          try
            let len = int_of_string len_str in
            let buf = String.make len ' ' in
              ignore (input ic buf 0 len);
              msg, buf
          with e ->
            let es = Printexc.to_string e in
            let bt = Printexc.get_backtrace () in
              raise (E.Worker_failure (E.Protocol_error (inp, es ^ ":" ^ bt)))

let master_msg_of = function
  | "ok", _ -> M_ok
  | "die", _ -> M_die
  | "ignore", _ -> M_ignore
  | "set", b ->
      (try M_settings (settings_of_bencode b)
       with _ -> raise (E.Worker_failure (E.Bad_msg ("set", b))))
  | "tsk", b ->
      (try M_taskinfo (taskinfo_of_bencode b)
       with _ -> raise (E.Worker_failure (E.Bad_msg ("tsk", b))))
  | "inp", b ->
      (try
         let status, inputs = inputs_of_bencode b in
         M_inputs (status, inputs)
       with _ -> raise (E.Worker_failure (E.Bad_msg ("inp", b))))
  | m, b ->
      raise (E.Worker_failure (E.Unknown_msg (m, b)))

let next_master_msg ic =
  let msg, payload = get_raw_master_msg ic in
    master_msg_of (String.lowercase msg, B.of_string payload)

(* worker -> master *)

type output_type =
  | Data
  | Labeled
  | Persistent

let string_of_output_type = function
  | Data -> "disco"
  | Labeled -> "part"
  | Persistent -> "tag"

type output = {
  label : string option;
  filename : string;
  otype : output_type;
}

type worker_msg =
  | W_version of string
  | W_pid of int
  | W_settings
  | W_taskinfo
  | W_input of int list
  | W_input_failure of int * string
  | W_status of string
  | W_error of string
  | W_output of output
  | W_done

let prepare_msg = function
  | W_version s ->
      "VSN", B.to_string (B.String s)
  | W_pid pid ->
      "PID", B.to_string (B.String (string_of_int pid))
  | W_settings ->
      "SET", B.to_string (B.String "")
  | W_taskinfo ->
      "TSK", B.to_string (B.String "")
  | W_input exclude_list ->
      let el = List.map (fun i -> B.Int (Int64.of_int i)) exclude_list in
        "INP", B.to_string (B.List el)
  | W_input_failure (_id, msg) ->
      "DAT", B.to_string (B.String msg)
  | W_status s ->
      "STA", B.to_string (B.String s)
  | W_error s ->
      "ERR", B.to_string (B.String s)
  | W_output o ->
      let m = B.List ([B.String o.filename;
                       B.String (string_of_output_type o.otype);
                      ] @ (match o.label with
                             | None -> []
                             | Some l -> [B.String l])) in
      "OUT", B.to_string m
  | W_done ->
      "END", B.to_string (B.String "")

let version = "00"

let send_msg m oc =
  let tm = U.localtime (U.time ()) in
  let ts = (Printf.sprintf "%02d/%02d/%02d %02d:%02d:%02d"
              (tm.U.tm_year - 100) tm.U.tm_mon tm.U.tm_mday
              tm.U.tm_hour tm.U.tm_min tm.U.tm_sec) in
  let tag, payload = prepare_msg m in
    Printf.fprintf oc "**<%s:%s> %s \n%s\n<>**\n"
      tag version ts payload

(* synchronous msg exchange / rpc *)

let send_request m ic oc =
  send_msg m oc;
  flush oc;
  next_master_msg ic
