(* Basic functions to grep words out from ascii text files. *)

let string_split s c =
  let slen = String.length s in
  let rec iter cursor acc =
    if cursor >= slen
    then List.rev (List.filter (fun s -> String.length s > 0) acc)
    else (try
            let pivot = String.index_from s cursor c in
            iter (pivot + 1) (String.sub s cursor (pivot - cursor) :: acc)
          with Not_found ->
            iter slen (String.sub s cursor (slen - cursor) :: acc))
  in iter 0 []

let is_whitespace = function
  | ' ' | '\t' | '\n' | '\r' -> true
  | _ -> false

let strip_word = function
  | "" -> ""
  | w ->
      let len = String.length w in
      let rec starter ofs =
        if ofs = len then len - 1
        else if is_whitespace w.[ofs] then starter (ofs + 1)
        else ofs in
      let rec ender ofs =
        if ofs = 0 then 0
        else if is_whitespace w.[ofs] then ender (ofs - 1)
        else ofs in
      let s = starter 0 in
      let e = ender (len - 1) in
        match s,e with
          | _ when s >= e -> ""
          | 0, _ when e = len - 1 -> w
          | _ -> String.sub w s (e - s + 1)

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

let label_of : string -> Pipeline.label = fun word ->
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
      Wc_b.write_wc (output_of init disco (label_of k)) (k, v);
    ) init.dict;
  Hashtbl.iter (fun _l o -> Bi_outbuf.flush_channel_writer o) init.outputs;
  disco.Task.log (Printf.sprintf "Mapped %d entries into %d files.\n"
                 !(init.count) (Hashtbl.length init.outputs))

module MapTask = struct
  module T = Task

  type init = t

  let task_init = task_init

  let task_process init disco in_chan =
    disco.T.log (Printf.sprintf
                   "Mapping %s (%d bytes) with label %d on %s ...\n"
                   disco.T.input_url disco.T.input_size
                   disco.T.group_label disco.T.hostname);

    (* The inputs are raw data files, so we need to parse them
       ourselves. *)
    let rec loop () =
      List.iter
        (fun w ->
          match strip_word w with
          | "" ->
              ()
          | key ->
              let v = try Hashtbl.find init.dict key with Not_found -> 0 in
              Hashtbl.replace init.dict key (v + 1);
              incr init.count
        ) (string_split (input_line in_chan) ' ');
      loop ()
    in try loop () with End_of_file -> ()

  let task_done = task_done
end

let sHUFFLE_BUFFER_SIZE = 5*1024*1024

module ReduceTask = struct
  module T = Task

  type init = t

  let task_init = task_init

  let task_process init disco in_chan =
    disco.T.log (Printf.sprintf
                   "Mapping %s (%d bytes) with label %d on %s ...\n"
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
    in try loop () with End_of_file -> ()

  let task_done = task_done
end

let _ =
  Worker.start [("map",     (module MapTask     : Task.TASK));
                ("shuffle", (module ReduceTask  : Task.TASK));
                ("reduce",  (module ReduceTask  : Task.TASK))]
