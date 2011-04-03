module J = Json
module JC = Json_conv
module JP = Json_parse
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

type task_input_status =
  | Task_input_more
  | Task_input_done

let task_input_status_of_string s =
  match String.lowercase s with
    | "more" -> Task_input_more
    | "done" -> Task_input_done
    | _      -> raise (E.Worker_failure (E.Unexpected_msg ("task_input ('" ^ s ^ "')")))

type input_status =
  | Input_ok
  | Input_failed

let input_status_of_string s =
  match String.lowercase s with
    | "ok"     -> Input_ok
    | "failed" -> Input_failed
    | _        -> raise (E.Worker_failure (E.Unexpected_msg ("inputs ('" ^ s ^ "')")))

type input_id = int
type replica_id = int
type replica = replica_id * string
type input = input_id * input_status * replica list

type master_msg =
  | M_ok
  | M_die
  | M_ignore
  | M_settings of settings
  | M_taskinfo of taskinfo
  | M_task_input of task_input_status * input list
  | M_retry of replica list
  | M_fail

let master_msg_name = function
  | M_ok -> "ok"
  | M_die -> "die"
  | M_ignore -> "ignore"
  | M_taskinfo _ -> "taskinfo"
  | M_settings _ -> "settings"
  | M_task_input _ -> "task_input"
  | M_retry _ -> "retry"
  | M_fail -> "fail"

let split s =
  let indx = (try Some (String.index s ' ') with _ -> None) in
    match indx with
      | None -> s, ""
      | Some i ->
          let slen = String.length s in
            (String.sub s 0 i), (String.sub s (i+1) (slen - i - 1))

let settings_of b =
  let table = JC.to_object_table b in
  let lookup key = JC.object_field table key in
  let settings_port = JC.to_int (lookup "port") in
  let settings_put_port = JC.to_int (lookup "put_port") in
    { settings_port; settings_put_port }

let taskinfo_of b =
  let table = JC.to_object_table b in
  let lookup key = JC.object_field table key in
  let task_id = JC.to_int (lookup "taskid") in
  let task_stage = stage_of_string (JC.to_string (lookup "mode")) in
  let task_name = JC.to_string (lookup "jobname") in
  let task_host = JC.to_string (lookup "host") in
  let task_rootpath = "./" in
    { task_id; task_stage; task_name; task_host; task_rootpath }

let task_input_of b =
  let msg = JC.to_list b in
  let status = task_input_status_of_string (JC.to_string (List.hd msg)) in
  let minps = JC.to_list (List.hd (List.tl msg)) in
  let mk_inp =
    (fun l ->
       let l = JC.to_list l in
       let inp_id = JC.to_int (List.hd l) in
       let inp_status = input_status_of_string (JC.to_string (List.nth l 1)) in
       let replicas = JC.to_list (List.nth l 2) in
       let inps = List.map (fun jlist ->
                              let l = JC.to_list jlist in
                              let rep_id = JC.to_int (List.hd l) in
                              let rep_url = JC.to_string (List.nth l 1) in
                                (rep_id, rep_url)
                           ) replicas in
         inp_id, inp_status, inps) in
    status, List.map mk_inp minps

let retry_of b =
  List.map (fun jlist ->
              let l = JC.to_list jlist in
                JC.to_int (List.hd l), JC.to_string (List.nth l 1)
           ) (JC.to_list b)

(* The master should respond within this time. *)
let tIMEOUT = 5.0 *. 60.0 (* in seconds *)

let bUFLEN = 1024
let rec get_raw_master_msg ic =
  let ifd = Unix.descr_of_in_channel ic in
  let timeout = Unix.gettimeofday () +. tIMEOUT in
  let get_timeout () =
    let curtime = Unix.gettimeofday () in
      if curtime < timeout then timeout -. curtime
      else raise (E.Worker_failure (E.Protocol_error "timeout")) in
  let msgbuf, buf, ofs = Buffer.create bUFLEN, String.make bUFLEN '\000', ref 0 in
  let msg_info = ref None in
  let process_prefix p =
    let msg, len_str = split p in
      try msg_info := Some (msg, int_of_string len_str)
      with e ->
        let es, bt = Printexc.to_string e, Printexc.get_backtrace () in
          raise (E.Worker_failure (E.Protocol_parse_error (p, es ^ ":" ^ bt))) in
  let msg_done () = (match !msg_info with
                       | None -> false
                       | Some (_, len) -> len < Buffer.length msgbuf) in
  let return_val () = (match !msg_info with
                         | Some (msg, len) -> msg, Buffer.sub msgbuf 0 len
                         | None -> assert false) in
  let rec do_read () =
    let len = Unix.read ifd buf !ofs (String.length buf - !ofs) in
      match !msg_info with
        | None ->
            (match (try Some (String.index_from buf !ofs '\n') with Not_found -> None) with
               | Some nl -> (Buffer.add_substring msgbuf buf (nl+1) (!ofs + len - nl - 1);
                             ofs := 0;
                             process_prefix (String.sub buf 0 nl))
               | None -> ofs := !ofs + len)
        | Some (_, _msglen) ->
            assert (!ofs = 0);
            Buffer.add_substring msgbuf buf !ofs (!ofs + len) in
  let rec loop () =
    match Unix.select [ifd] [] [ifd] (get_timeout ()) with
      | _, _, [_] -> raise (E.Worker_failure (E.Protocol_error "socket error"))
      | [_], _, _ -> do_read (); if msg_done () then return_val () else loop ()
      | _, _, _ -> loop ()
  in loop ()

let master_msg_of = function
  | "ok", _     -> M_ok
  | "die", _    -> M_die
  | "ignore", _ -> M_ignore
  | "set", j    -> M_settings (settings_of j)
  | "tsk", j    -> M_taskinfo (taskinfo_of j)
  | "inp", j    -> (let status, inputs = task_input_of j in
                      M_task_input (status, inputs))
  | "retry", j  -> M_retry (retry_of j)
  | "fail", _   -> M_fail
  | m, j        -> raise (E.Worker_failure (E.Unknown_msg (m, j)))

let next_master_msg ic =
  let msg, payload = get_raw_master_msg ic in
    Utils.dbg "<- %s: %s" msg payload;
    try
      master_msg_of (String.lowercase msg, JP.of_string payload)
    with
      | JP.Parse_error e ->
          raise (E.Worker_failure (E.Protocol_parse_error (payload, JP.string_of_error e)))
      | JC.Json_conv_error e ->
          raise (E.Worker_failure (E.Bad_msg (msg, payload, JC.string_of_error e)))
      | e ->
          raise e

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
  | W_input_exclude of int list
  | W_input_restrict of int list
  | W_input_failure of int * int list
  | W_status of string
  | W_error of string
  | W_output of output
  | W_done

let prepare_msg = function
  | W_version s ->
      "VSN", J.to_string (J.String s)
  | W_pid pid ->
      "PID", J.to_string (J.String (string_of_int pid))
  | W_settings ->
      "SET", J.to_string (J.String "")
  | W_taskinfo ->
      "TSK", J.to_string (J.String "")
  | W_input_exclude exclude_list ->
      let exclude = J.Array (Array.of_list (List.map JC.of_int exclude_list)) in
        "INP", J.to_string (J.Object [| "exclude", exclude |])
  | W_input_restrict restrict_list ->
      let restrict = J.Array (Array.of_list (List.map JC.of_int restrict_list)) in
        "INP", J.to_string (J.Object [| "restrict", restrict |])
  | W_input_failure (input_id, rep_ids) ->
      let failed_replicas = Array.of_list (List.map JC.of_int rep_ids) in
      "DAT", J.to_string (J.Array [| JC.of_int input_id; J.Array failed_replicas |])
  | W_status s ->
      "STA", J.to_string (J.String s)
  | W_error s ->
      "ERR", J.to_string (J.String s)
  | W_output o ->
      let list = [J.String o.filename;
                  J.String (string_of_output_type o.otype);
                 ] @ (match o.label with
                        | None -> []
                        | Some l -> [J.String l]) in
        "OUT", J.to_string (J.Array (Array.of_list list))
  | W_done ->
      "END", J.to_string (J.String "")

let version = "00"

let send_msg m oc =
  let tm = U.localtime (U.time ()) in
  let ts = (Printf.sprintf "%02d/%02d/%02d %02d:%02d:%02d"
              (tm.U.tm_year - 100) tm.U.tm_mon tm.U.tm_mday
              tm.U.tm_hour tm.U.tm_min tm.U.tm_sec) in
  let tag, payload = prepare_msg m in
    Utils.dbg "-> %s: %s" tag payload;
    Printf.fprintf oc "**<%s:%s> %s \n%s\n<>**\n"
      tag version ts payload

(* synchronous msg exchange / rpc *)

let send_request m ic oc =
  send_msg m oc;
  flush oc;
  next_master_msg ic
