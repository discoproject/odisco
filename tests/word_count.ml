let char_in_string s c =
  let len = String.length s in
  let rec loop i = (if i = len then false
                else if s.[i] = c then true
                else loop (i + 1)) in
    loop 0

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
  output_string oc (Printf.sprintf "%s\xff%s\x00" key value)

module MapTask = struct
  module T = Task
  type init = int ref * (string, int) Hashtbl.t

  let task_init _ =
    ref 0, (Hashtbl.create (1024 * 1024) : (string, int) Hashtbl.t)

  let task_process (cnt, tbl) disco in_chan =
    disco.T.log (Printf.sprintf
                   "Mapping %s (%d bytes) with label %d on %s ...\n"
                   disco.T.input_url disco.T.input_size
                   disco.T.group_label disco.T.hostname);
    let rec loop () =
      List.iter (fun w ->
                   match strip_word w with
                     | "" ->
                       ()
                     | key ->
                       let v = try Hashtbl.find tbl key with Not_found -> 0 in
                       Hashtbl.replace tbl key (v + 1);
                       incr cnt
                ) (string_split (input_line in_chan) ' ');
      loop ()
    in try loop () with End_of_file -> ()

  let task_done (cnt, tbl) disco =
    Hashtbl.iter
      (fun k v ->
        output (disco.T.out_channel ~label:0) k (Printf.sprintf "%d" v)
      ) tbl;
    disco.T.log (Printf.sprintf "Mapped %d entries.\n" !cnt)
end

module ReduceTask = struct
  module T = Task
  type init = string list ref

  let task_init _ = ref ([] : string list)

  let task_process in_files disco _ =
    in_files := disco.Task.input_path :: !in_files

  (* Invoke external sort *)
  let unix_sort in_files out_file disco =
    let sort_env = Array.append (Unix.environment ()) [|"LC_ALL=C"|] in
    let sort_args = Array.of_list (["-z";
                                    "-t"; "\xff";
                                    "-k"; "1,1";
                                    "-T"; disco.Task.temp_dir;
                                    "-S"; "15%";
                                    "-o"; out_file]
                                   @ in_files) in
      match Unix.fork () with
        | 0 ->
            Unix.execvpe "sort" sort_args sort_env
        | child ->
             (let _, stat = Unix.waitpid [ Unix.WUNTRACED ] child in
                match stat with
                  | Unix.WEXITED 0 -> ()
                  | Unix.WEXITED code ->
                      failwith (Printf.sprintf "sort failure: exit %d" code)
                  | Unix.WSIGNALED sg ->
                      failwith (Printf.sprintf "sort failure: signal %d" sg)
                  | Unix.WSTOPPED sg ->
                      failwith (Printf.sprintf "sort failure: stop %d" sg)
             )

  type sorted_input = {
    chan : in_channel;
    buf : Buffer.t;
    mutable next_record : int;
    mutable records_parsed : int;
  }
  let rECORD_END    = '\x00'
  let lINE_BUF_SIZE = 2 * 2 * 1024 * 1024
  let bUF_SIZE  = 2 * 128 * 1024 * 1024
  let mAX_BUF_SIZE = 2 * 512 * 1024 * 1024

  let init_sorted_input input =
    {chan = input; buf = Buffer.create bUF_SIZE; next_record = 0; records_parsed = 0}

  let rec get_record_end_loop buf ofs =
    if Buffer.nth buf ofs = rECORD_END then ofs
    else get_record_end_loop buf (ofs + 1)

  let rec get_record_end si =
    let next_record = si.next_record in
    match (try get_record_end_loop si.buf next_record
           with Invalid_argument _ -> -1)
    with
      | -1 ->  (* reload buffer and retry *)
          let s = String.create lINE_BUF_SIZE in begin
              match input si.chan s 0 lINE_BUF_SIZE with
                | 0 -> raise End_of_file
                | read -> (Buffer.add_string si.buf (String.sub s 0 read);
                           get_record_end si)
            end
      | ofs -> (* check if we should free memory *)
          let bufsize = Buffer.length si.buf in
            if bufsize > mAX_BUF_SIZE then begin
              let tmp = Buffer.sub si.buf next_record (bufsize - next_record) in
                Buffer.reset si.buf;
                Buffer.add_string si.buf tmp;
                si.next_record <- 0;
                ofs - next_record
            end else ofs

  let get_record si =
    let ofs = get_record_end si in
    let r = (if ofs > si.next_record
             then Buffer.sub si.buf si.next_record (ofs - si.next_record)
             else "")
    in
      si.next_record <- ofs + 1;
      si.records_parsed <- si.records_parsed + 1;
      r

  let task_done in_files disco =
    let sort_out = Filename.temp_file ~temp_dir:disco.Task.temp_dir "sorted-" "" in
      disco.Task.log (Printf.sprintf "Starting sort (=>%s)\n" sort_out);
      unix_sort !in_files sort_out disco;
      disco.Task.log "Sort done\n";
      let si = init_sorted_input (open_in sort_out) in
      let task_out = disco.Task.out_channel ~label:0 in
      let rec loop word count =
        let kv = string_split (get_record si) '\xff' in
        match List.length kv with
          | 0 | 1 -> loop word count
          | _ ->
            let k, v = (List.hd kv), int_of_string (List.nth kv 1) in
            if k = word then loop word (count + v)
            else if word = "" then loop k v
            else begin
              output task_out word (Printf.sprintf "%d" count);
              loop k v
            end
      in
        try loop "" 0
        with End_of_file ->
          disco.Task.log (Printf.sprintf "Reduced %d records\n" si.records_parsed)
end

let _ =
  Worker.start [("map",    (module MapTask : Task.TASK));
                ("reduce", (module ReduceTask : Task.TASK))]
