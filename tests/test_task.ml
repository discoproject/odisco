module U = Utils

module TestTask = struct
  type map_init = int ref

  let map_init _ = ref 0

  let map cnt disco in_chan =
    disco.Task.log (Printf.sprintf "Mapping %s (%d bytes) on %s ...\n"
                      disco.Task.input_url disco.Task.input_size disco.Task.hostname);
    let rec loop () =
      List.iter (fun w ->
                   match U.strip_word w with
                     | "" -> ()
                     | key -> disco.Task.output ~key "1"; incr cnt
                ) (U.string_split (input_line in_chan) ' ');
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
      let kv = U.string_split (input_line in_chan) ' ' in
      let k, v = (List.hd kv), int_of_string (List.nth kv 1) in
        Hashtbl.replace tbl k ((lookup k) + v);
        loop ()
    in
      try loop () with End_of_file -> ()

  let reduce_done tbl disco =
    Hashtbl.iter (fun k v -> disco.Task.output k (Printf.sprintf "%d" v)) tbl;
    disco.Task.log (Printf.sprintf "Reduce output %d entries.\n" (Hashtbl.length tbl))

end

let _ =
  Worker.test_start (module TestTask : Task.TASK)
