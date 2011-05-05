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

let output oc ~key value =
  output_string oc (Printf.sprintf "%s %s\n" key value)

module TestTask = struct
  type map_init = int ref

  let map_init _ = ref 0

  let map cnt disco in_chan =
    disco.Task.log (Printf.sprintf "Mapping %s (%d bytes) on %s ...\n"
                      disco.Task.input_url disco.Task.input_size disco.Task.hostname);
    let rec loop () =
      List.iter (fun w ->
                   match strip_word w with
                     | "" -> ()
                     | key -> output (disco.Task.out_channel ~label:None) ~key "1"; incr cnt
                ) (string_split (input_line in_chan) ' ');
      loop ()
    in try loop () with End_of_file -> ()

  let map_done cnt disco =
    disco.Task.log (Printf.sprintf "Mapped %d entries.\n" !cnt)

  type reduce_init = (string, int) Hashtbl.t

  let reduce_init _ = (Hashtbl.create 4096 : (string, int) Hashtbl.t)

  let reduce tbl disco in_chan =
    disco.Task.log (Printf.sprintf "Reducing %s (%d bytes) on %s ...\n"
                      disco.Task.input_url disco.Task.input_size disco.Task.hostname);
    let lookup w = try Hashtbl.find tbl w with Not_found -> 0 in
    let rec loop () =
      let kv = string_split (input_line in_chan) ' ' in
      let k, v = (List.hd kv), int_of_string (List.nth kv 1) in
        Hashtbl.replace tbl k ((lookup k) + v);
        loop ()
    in
      try loop () with End_of_file -> ()

  let reduce_done tbl disco =
    Hashtbl.iter (fun k v ->
                    output (disco.Task.out_channel ~label:None) k (Printf.sprintf "%d" v)
                 ) tbl;
    disco.Task.log (Printf.sprintf "Reduce output %d entries.\n" (Hashtbl.length tbl))

end

let _ =
  Worker.start (module TestTask : Task.TASK)
