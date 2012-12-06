module J = Json
module JC = Json_conv
module JP = Json_parse
module L = Pipeline
module E = Errors
module U = Utils

let protocol_version = "1.1"

(* master -> worker *)

type taskinfo = {
  task_jobname : string;
  task_jobfile : string;

  task_stage : L.stage;
  task_grouping : L.grouping;
  task_group_label : L.label;
  task_group_node : string option;
  task_id : int;

  task_host : string;

  task_master : string;
  task_disco_port : int;
  task_put_port : int;
  task_disco_root : string;
  task_ddfs_root : string;
  task_rootpath : string;
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

type task_input_status =
  | Task_input_more
  | Task_input_done

let task_input_status_of_string s =
  match String.lowercase s with
  | "more" -> Task_input_more
  | "done" -> Task_input_done
  | _      -> raise (E.Worker_failure
                       (E.Unexpected_msg ("task_input ('" ^ s ^ "')")))

type input_status =
  | Input_ok
  | Input_failed

let input_status_of_string s =
  match String.lowercase s with
  | "ok"     -> Input_ok
  | "failed" -> Input_failed
  | _        -> raise (E.Worker_failure
                         (E.Unexpected_msg ("inputs ('" ^ s ^ "')")))

type input_id = int
type replica_id = int
type replica = replica_id * Uri.t
type input = input_id * input_status * replica list

type master_msg =
  | M_ok
  | M_die
  | M_taskinfo of taskinfo
  | M_task_input of task_input_status * input list
  | M_retry of replica list
  | M_fail

let master_msg_name = function
  | M_ok -> "ok"
  | M_die -> "die"
  | M_taskinfo _ -> "taskinfo"
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

let taskinfo_of b =
  let table = JC.to_object_table b in
  let lookup key = JC.object_field table key in

  let task_jobname = JC.to_string (lookup "jobname") in
  let task_jobfile = JC.to_string (lookup "jobfile") in

  let task_stage = JC.to_string (lookup "stage") in
  let task_grouping = L.grouping_of_string (JC.to_string (lookup "grouping")) in
  let group = JC.to_list (lookup "group") in
  let task_group_label = JC.to_int (List.hd group) in
  let task_group_node =
    match JC.to_string (List.nth group 1) with
    | "none" -> None
    | node   -> Some node in
  let task_id = JC.to_int (lookup "taskid") in

  let task_host = JC.to_string (lookup "host") in
  let task_master = JC.to_string (lookup "master") in
  let task_disco_port = JC.to_int (lookup "disco_port") in
  let task_put_port = JC.to_int (lookup "put_port") in
  let task_disco_root = JC.to_string (lookup "disco_data") in
  let task_ddfs_root = JC.to_string (lookup "ddfs_data") in
  let task_rootpath = (Printf.sprintf "./.%s-%d-%f"
                         task_stage task_id (Unix.time ())) in
  {task_jobname; task_jobfile;
   task_stage; task_grouping; task_group_label; task_group_node; task_id;
   task_host; task_master; task_disco_port; task_put_port;
   task_disco_root; task_ddfs_root; task_rootpath}

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
      let inps = List.map
          (fun jlist ->
            let l = JC.to_list jlist in
            let rep_id = JC.to_int (List.hd l) in
            let rep_url = Uri.of_string (JC.to_string (List.nth l 1)) in
            (rep_id, rep_url)
          ) replicas in
      inp_id, inp_status, inps) in
  status, List.map mk_inp minps

let retry_of b =
  List.map
    (fun jlist ->
      let l = JC.to_list jlist in
      let rid = JC.to_int (List.hd l) in
      let url = Uri.of_string (JC.to_string (List.nth l 1)) in
      rid, url
    ) (JC.to_list b)

(* The master should respond within this time. *)
let tIMEOUT = 5.0 *. 60.0 (* in seconds *)

(* The format of messages going in both directions is:

   <tag> 'SP' <payload-len> 'SP' <payload> '\n'

   Both <tag> and <payload-len> are required to be less than 10
   characters each.
*)

let bUFLEN = 1024
let get_raw_master_msg ic =
  let msg = ref None in
  let process_prefix p =
    let tag, rem = split p in
    let len, rem = split rem in
    try msg := Some (tag, int_of_string len); rem
    with e ->
      let es, bt = Printexc.to_string e, Printexc.get_backtrace () in
      raise (E.Worker_failure (E.Protocol_parse_error (p, es ^ ":" ^ bt))) in

  let payload, buf, ofs =
    Buffer.create bUFLEN, String.make bUFLEN '\000', ref 0 in
  let return_msg () =
    (match !msg with
     | Some (tag, len) ->
         if len < Buffer.length payload
        then Some (tag, Buffer.sub payload 0 len)
         else None
     | None -> None) in

  let ifd = Unix.descr_of_in_channel ic in
  let do_read () =
    let len = Unix.read ifd buf !ofs (String.length buf - !ofs) in
    match !msg with
    | None ->
        ofs := !ofs + len;
        if !ofs >= 22 || String.rcontains_from buf !ofs '\n' then begin
          Buffer.add_string payload (process_prefix buf);
          ofs := 0
        end
    | Some (_, _) ->
        assert (!ofs = 0);
        Buffer.add_substring payload buf 0 len in

  let timeout = Unix.gettimeofday () +. tIMEOUT in
  let get_timeout () =
    let curtime = Unix.gettimeofday () in
    if curtime < timeout then timeout -. curtime
    else raise (E.Worker_failure (E.Protocol_error "timeout")) in
  let rec loop () =
    match Unix.select [ifd] [] [ifd] (get_timeout ()) with
    | _, _, [_] ->
        raise (E.Worker_failure (E.Protocol_error "socket error"))
    | [_], _, _ ->
        do_read (); (match return_msg() with Some m -> m | None -> loop ())
    | _, _, _ ->
        loop ()
  in loop ()

let master_msg_of = function
  | "OK", _     -> M_ok
  | "DIE", _    -> M_die
  | "TASK", j   -> M_taskinfo (taskinfo_of j)
  | "INPUT", j  -> let status, inputs = task_input_of j in
                   M_task_input (status, inputs)
  | "RETRY", j  -> M_retry (retry_of j)
  | "FAIL", _   -> M_fail
  | m, j        -> raise (E.Worker_failure (E.Unknown_msg (m, j)))

let next_master_msg ic =
  let msg, payload = get_raw_master_msg ic in
  U.dbg "<- %s: %s" msg payload;
  try master_msg_of (msg, JP.of_string payload)
  with
  | JP.Parse_error e ->
      raise (E.Worker_failure
               (E.Protocol_parse_error (payload, JP.string_of_error e)))
  | JC.Json_conv_error e ->
      raise (E.Worker_failure
               (E.Bad_msg (msg, payload, JC.string_of_error e)))
  | e ->
      raise e

(* worker -> master *)

type output = {
  label : int;
  filename : string;
}

type worker_msg =
  | W_worker of (* version *) string * (* pid *) int
  | W_taskinfo
  | W_input_exclude of input_id list
  | W_input_include of input_id list
  | W_input_failure of input_id * replica_id list
  | W_message of string
  | W_error of string
  | W_fatal of string
  | W_output of output * int
  | W_done

let prepare_msg = function
  | W_worker (v, pid) ->
      let p = Int64.of_int pid in
      "WORKER", J.to_string (J.Object [|"version", J.String v; "pid", J.Int p|])
  | W_taskinfo ->
      "TASK", J.to_string (J.String "")
  | W_input_exclude exclude_list ->
      let exclude = J.Array (Array.of_list (List.map JC.of_int exclude_list)) in
      "INPUT", J.to_string (J.Array [|J.String "exclude"; exclude|])
  | W_input_include include_list ->
      let incl = J.Array (Array.of_list (List.map JC.of_int include_list)) in
      "INPUT", J.to_string (J.Array [|J.String "include"; incl|])
  | W_input_failure (input_id, rep_ids) ->
      let failed_reps = J.Array (Array.of_list (List.map JC.of_int rep_ids)) in
      "INPUT_ERR", J.to_string (J.Array [|JC.of_int input_id; failed_reps|])
  | W_message s ->
      "MSG", J.to_string (J.String s)
  | W_error s ->
      "ERROR", J.to_string (J.String s)
  | W_fatal s ->
      "FATAL", J.to_string (J.String s)
  | W_output (o, sz) ->
      let list = [ J.Int (Int64.of_int o.label);
                   J.String o.filename;
                   J.Int (Int64.of_int sz) ]
      in "OUTPUT", J.to_string (J.Array (Array.of_list list))
  | W_done ->
      "DONE", J.to_string (J.String "")

let send_msg m oc =
  let tag, payload = prepare_msg m in
  U.dbg "-> %s: %s" tag payload;
  Printf.fprintf oc "%s %d %s\n" tag (String.length payload) payload

(* synchronous msg exchange / rpc *)

let send_request m ic oc =
  send_msg m oc;
  flush oc;
  next_master_msg ic

(* parsing the contents of an index *)

let parse_index s =
  let parse_line l =
    match U.string_split l ' ' with
    | label :: url :: size :: [] ->
        (int_of_string label), (url, (int_of_string size))
    | _ -> assert false
  in List.map parse_line (U.string_split s '\n')
