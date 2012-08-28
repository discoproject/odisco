(* Basic functions to grep words out from ascii text files. *)
let is_whitespace = function
  | ' ' | '\t' | '\n' | '\r' -> true
  | _ -> false

let words_of_string s =
  let len = String.length s in
  let finish acc = List.rev acc in
  let rec iter acc = function
    | `Skip next_ofs when next_ofs >= len ->
        finish acc
    | `Skip next_ofs ->
        if is_whitespace s.[next_ofs]
        then iter acc (`Skip (next_ofs + 1))
        else iter acc (`Collect (next_ofs, next_ofs + 1))
    | `Collect (start_ofs, next_ofs) when next_ofs >= len ->
        finish ((String.sub s start_ofs (next_ofs - start_ofs)) :: acc)
    | `Collect (start_ofs, next_ofs) ->
        if is_whitespace s.[next_ofs]
        then iter (String.sub s start_ofs (next_ofs - start_ofs) :: acc)
            (`Skip (next_ofs + 1))
        else iter acc (`Collect (start_ofs, (next_ofs + 1)))
  in iter [] (`Skip 0)

(* Common utilities for task stages *)

let mAX_LABEL = 10

type t = {
    count : int ref;
    dict : (string, int) Hashtbl.t;
    outputs : (Pipeline.label, Bi_outbuf.t) Hashtbl.t;
}

let task_init _ = {
  count = ref 0;
  dict = (Hashtbl.create (1024 * 1024) : (string, int) Hashtbl.t);
  outputs = (Hashtbl.create mAX_LABEL
               : (Pipeline.label, Bi_outbuf.t) Hashtbl.t);
}

let mAP     = "map"
let sHUFFLE = "shuffle"
let rEDUCE  = "reduce"

let label_of disco word =
  if disco.Task.grouping = Pipeline.Group_all then
    0
  else
    let h = ref 0 in
    String.iter (fun c -> h := !h + Char.code c) word;
    !h mod mAX_LABEL

let output_of init disco label =
  try Hashtbl.find init.outputs label
  with Not_found ->
    let o = Bi_outbuf.create_channel_writer (disco.Task.out_channel ~label)
    in Hashtbl.add init.outputs label o;
    o

let task_done init disco =
  (* We put intermediate word counts into biniou files. *)
  Hashtbl.iter
    (fun k v ->
      Wc_b.write_wc (output_of init disco (label_of disco k)) (k, v);
    ) init.dict;
  Hashtbl.iter (fun _l o -> Bi_outbuf.flush_channel_writer o) init.outputs;
  disco.Task.log (Printf.sprintf "Output %d entries (%d keys) into %d files.\n"
                    !(init.count) (Hashtbl.length init.dict)
                    (Hashtbl.length init.outputs))

module MapTask = struct
  module T = Task

  type init = t

  let task_init = task_init

  let task_process init disco in_chan =
    disco.T.log (Printf.sprintf
                   "Processing %s (%d bytes) with label %d on %s ...\n"
                   disco.T.input_url disco.T.input_size
                   disco.T.group_label disco.T.hostname);

    (* The inputs are raw data files, so we need to parse them
       ourselves. *)
    let rec loop () =
      List.iter
        (fun key ->
          let v = try Hashtbl.find init.dict key with Not_found -> 0 in
          Hashtbl.replace init.dict key (v + 1);
          incr init.count
        ) (words_of_string (input_line in_chan));
      loop ()
    in try loop () with End_of_file -> ()

  let task_done = task_done
end

module ReduceTask = struct
  module T = Task

  type init = t

  let task_init = task_init

  let task_process init disco in_chan =
    disco.T.log (Printf.sprintf
                   "Processing %s (%d bytes) with label %d on %s ...\n"
                   disco.T.input_url disco.T.input_size
                   disco.T.group_label disco.T.hostname);

    (* The intermediate results are in biniou files. *)
    let inp = Bi_inbuf.from_channel in_chan in
    let rec loop () =
      let (k, v) = Wc_b.read_wc inp in
      let cnt = try Hashtbl.find init.dict k with Not_found -> 0 in
      Hashtbl.replace init.dict k (v + cnt);
      incr init.count;
      loop()
    in try loop () with End_of_file | Bi_inbuf.End_of_input -> ()

  let task_done = task_done
end

let _ =
  Worker.start [(mAP,     (module MapTask     : Task.TASK));
                (sHUFFLE, (module ReduceTask  : Task.TASK));
                (rEDUCE,  (module ReduceTask  : Task.TASK))]
