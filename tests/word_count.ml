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

  type reduce_init = string * out_channel

  let reduce_init disco =
    Filename.open_temp_file ~temp_dir:disco.Task.temp_dir "sortin-" ""

  let reduce (_, sort_chan) disco in_chan =
    disco.Task.log (Printf.sprintf "Preparing %s (%d bytes) for sort on %s ...\n"
                      disco.Task.input_url disco.Task.input_size disco.Task.hostname);
    let rec loop () =
      let kv = string_split (input_line in_chan) ' ' in
      let k, v = (List.hd kv), (List.nth kv 1) in
        (* Skip keys with embedded sort delimiters *)
        if (char_in_string k '\x00') || (char_in_string k '\xff') then
         loop ()
        else begin
          (* Write out in format suitable for unix sort *)
          Printf.fprintf sort_chan "%s\xff%s\x00" k v;
          loop ()
        end
    in
      try loop () with End_of_file -> ()

  (* Invoke external sort *)
  let unix_sort sort_file disco =
    let sort_env = Array.append (Unix.environment ()) [|"LC_ALL=C"|] in
    let sort_args = Array.of_list ["-z";
                                   "-t"; "\xff";
                                   "-k"; "1,1";
                                   "-T"; disco.Task.temp_dir;
                                   "-S"; "15%";
                                   "-o"; sort_file;
                                   sort_file] in
      match Unix.fork () with
        | 0 ->
            Unix.execvpe "sort" sort_args sort_env
        | child ->
             (let pid, stat = Unix.waitpid [ Unix.WUNTRACED ] child in
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
  let lINE_BUF_SIZE = 1024 * 2
  let mAX_BUF_SIZE  = 1024 * 1024

  let init_sorted_input input =
    {chan = input; buf = Buffer.create lINE_BUF_SIZE; next_record = 0; records_parsed = 0}

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

  let reduce_done (sort_file, sort_chan) disco =
    close_out sort_chan;
    disco.Task.log (Printf.sprintf "Starting sort (file=%s)\n" sort_file);
    unix_sort sort_file disco;
    disco.Task.log (Printf.sprintf "Sort done (file=%s)\n" sort_file);
    let si = init_sorted_input (open_in sort_file) in
    let task_out = disco.Task.out_channel ~label:None in
    let rec loop word count =
      let kv = string_split (get_record si) '\xff' in
      let k, v = (List.hd kv), int_of_string (List.nth kv 1) in
        if k = word then loop word (count + v)
        else if word = "" then loop k v
        else begin
          output task_out word (Printf.sprintf "%d" count);
          loop k v
        end
    in
      try loop "" 0
      with End_of_file -> disco.Task.log (Printf.sprintf "Reduced %d records\n" si.records_parsed)
end

let _ =
  Worker.start (module TestTask : Task.TASK)
