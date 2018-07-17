type ('a, 'b) lr =
  | Left of 'a  (* e.g. error result *)
  | Right of 'b (* e.g. valid result *)

let lrsplit list =
  let rec loop ((ll, rl) as acc) = function
    | [] -> acc
    | Left l :: rest  -> loop (l :: ll, rl) rest
    | Right r :: rest -> loop (ll, r :: rl) rest in
  let ll, rl = loop ([], []) list in
  List.rev ll, List.rev rl

let (+>) lrv f =
  match lrv with
  | Right v -> f v
  | Left e  -> Left e

(* handling options *)

let unopt = function
  | Some v -> v
  | None -> assert false

let defopt d = function
  | Some v -> v
  | None -> d

let mapopt f = function
  | Some v -> Some (f v)
  | None -> None

(* split strings at a separator character *)

let string_split s c =
  let slen = Bytes.length s in
  let rec iter cursor acc =
    if cursor >= slen then
      List.rev (List.filter (fun s -> Bytes.length s > 0) acc)
    else
      try
        let pivot = Bytes.index_from s cursor c in
        iter (pivot + 1) (Bytes.sub s cursor (pivot - cursor) :: acc)
      with Not_found ->
        iter slen (Bytes.sub s cursor (slen - cursor) :: acc)
  in iter 0 []

(* prefix handling *)

let is_prefix str p =
  let plen = Bytes.length p in
  let rec prefix_helper ofs =
    if str.[ofs] <> p.[ofs] then false
    else if ofs = 0 then true else prefix_helper (ofs - 1) in
  if Bytes.length str < plen then false else prefix_helper (plen - 1)

let strip_prefix str p =
  let plen = Bytes.length p in
  Bytes.sub str plen ((Bytes.length str) - plen)

(* whitespace handling and stripping *)

let is_whitespace = function
  | ' ' | '\t' | '\n' | '\r' -> true
  | _ -> false

let strip_word = function
  | "" -> ""
  | w ->
      let len = Bytes.length w in
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
      | _ -> Bytes.sub w s (e - s + 1)

(* debug logging *)
type log =
  | Nolog
  | Buf of Buffer.t
  | File of out_channel

let log = ref Nolog

let log_print s =
  let tm = Unix.localtime (Unix.gettimeofday ()) in
  Printf.sprintf "%d:%02d:%02d-%02d:%02d:%02d: %s\n%!"
    (tm.Unix.tm_year + 1900) tm.Unix.tm_mon tm.Unix.tm_mday
    tm.Unix.tm_hour tm.Unix.tm_min tm.Unix.tm_sec
    s

let verbose = ref true

let logger =
  match !verbose with
  | false ->
      ref (fun _s -> ())
  | true ->
      let buf = Buffer.create 2048 in
      log := Buf buf;
      ref (fun s -> Buffer.add_string buf (log_print s))

let init_logfile task_rootpath =
  if !verbose then begin
    match !log with
    | Nolog ->
        ()
    | Buf buf ->
        let logf = (open_out_gen [ Open_creat; Open_append; Open_wronly ]
                      0o660 (Filename.concat task_rootpath "oc.dbg")) in
        Printf.fprintf logf "%s%!" (Buffer.contents buf);
        log := File logf;
        logger := (fun s -> Printf.fprintf logf "%s%!" (log_print s))
    | File oldf ->
        Printf.fprintf oldf "Attempted to re-init logfile in %s!%!" task_rootpath
  end

let close_logfile () =
  match !log with
  | Nolog ->
      ()
  | Buf buf ->
      let logf = (open_out_gen [ Open_creat; Open_append; Open_wronly ]
                    0o660 "/tmp/oc.dbg") in
      Printf.fprintf logf "%s" (Buffer.contents buf);
      Printf.fprintf logf "(incomplete log)%!";
      close_out logf
  | File logf ->
      close_out logf

let dbg fmt = Printf.ksprintf !logger fmt

(* file contents *)

let contents_of_file f =
  (* Assumes the file is not being modified while it is being read *)
  let inc = open_in f in
  let sz = (Unix.stat f).Unix.st_size in
  let sbuf = Bytes.create sz in
  let rec slurp ofs len =
    let read = input inc sbuf ofs len in
    if read > 0 then
      slurp (ofs + read) (len - read) in
  slurp 0 sz;
  close_in inc;
  sbuf
