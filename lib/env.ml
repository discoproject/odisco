module P = Protocol
module E = Errors
module C = Http_client
module U = Utils

(* file wrapper *)

module File = struct
  type t = {
    name : string;
    fd   : Unix.file_descr;
    delete_on_close : bool;
  }
  let name f = f.name
  let fd f = f.fd

  let open_new ~delete_on_close name =
    let flags = [Unix.O_RDWR; Unix.O_CREAT; Unix.O_TRUNC] in
    let fd = Unix.openfile name flags 0o640
    in {name; fd; delete_on_close}

  let open_existing name =
    let flags = [Unix.O_RDONLY] in
    let fd = Unix.openfile name flags 0o640
    in {name; fd; delete_on_close = false}

  let close f =
    (try Unix.close f.fd
     with Unix.Unix_error (Unix.EBADF, _, _) -> ());
    if f.delete_on_close then Unix.unlink f.name

  let size f =
    try (Unix.stat f.name).Unix.st_size
    with Unix.Unix_error (_,_,_) -> 0
end

(* internal decompression utilities *)

let is_gzipped url =
  let len = String.length url in
  if len < 3 (* ".gz" *) then false
  else String.sub url (len - 3) 3 = ".gz"

let filesize fname = (Unix.stat fname).Unix.st_size

let gzip_content fname =
  let zsz = filesize fname in
  let inc = Gzip.open_in fname in
  let buf = Buffer.create zsz in
  let sbuf = Bytes.create zsz in
  let rec slurp () =
    let nbytes = Gzip.input inc sbuf 0 zsz in
    if nbytes > 0 then begin
      Buffer.add_substring buf sbuf 0 nbytes;
      slurp ()
    end in
  slurp ();
  Gzip.close_in inc;
  Buffer.contents buf

let gunzip_str s url =
  let (tname, toc) =
    Filename.open_temp_file ~temp_dir:"/tmp" "tmp-" (Filename.basename url) in
  output_string toc s;
  close_out toc;
  gzip_content tname

let content_of local_file =
  if is_gzipped local_file
  then gzip_content local_file
  else U.contents_of_file local_file

(* internal utilities to handle URLs pointing inside local Disco filesystem *)

let dISCO_PATH = "/disco/"
let dDFS_PATH  = "/ddfs/"

let is_known_path = function
  | None   -> false
  | Some p -> U.is_prefix p dISCO_PATH || U.is_prefix p dDFS_PATH

let find_local taskinfo replicas =
  (* A locally hosted url either has (i) a "file://" scheme, or (ii) a
     host portion that matches the host the task is running on, and a
     path with a known Disco prefix. *)
  let locals = List.filter
      (fun u ->
        (u.Uri.scheme = Some "file")
         || ((match u.Uri.authority with
              | None   -> false
              | Some a -> a.Uri.host = taskinfo.P.task_host
             ) && (is_known_path u.Uri.path)))
      replicas in
  match locals with
  | []     -> None
  | l :: _ -> Some l

(* This assumes that the argument uri has been checked to be a local
   file using 'find_local'. *)
let local_file_of taskinfo uri =
  if uri.Uri.scheme = Some "file"
  then U.unopt uri.Uri.path
  else
    let uri_path = U.unopt uri.Uri.path in
    if U.is_prefix uri_path dISCO_PATH then
      let p = U.strip_prefix uri_path dISCO_PATH in
      Filename.concat taskinfo.P.task_disco_root p
    else
      let p = U.strip_prefix uri_path dDFS_PATH in
      Filename.concat taskinfo.P.task_ddfs_root p

(* input retrieval utilities *)

type 'a input_location =
  | Remote of P.input_id * 'a * Uri.t list
  | Local  of P.input_id * 'a * Uri.t * (* filename *) string

let try_localize taskinfo (iid, cid, replicas) =
  match (find_local taskinfo replicas) with
  | None   -> Remote (iid, cid, replicas)
  | Some l -> Local  (iid, cid, l, local_file_of taskinfo l)

(* create a local filename for a remote file *)

let local_filename taskinfo uri =
  let basename =
    match uri.Uri.path, uri.Uri.authority with
    | None, None   | Some "", None ->
        "remote_file"
    | None, Some a | Some "", Some a ->
        a.Uri.host
    | Some p, None ->
        Filename.basename p
    | Some p, Some a ->
        a.Uri.host ^ "_" ^ Filename.basename p
  in Filename.concat taskinfo.P.task_rootpath basename

(* retrieval requests *)

type 'a input_req = P.input_id * 'a * Uri.t list

(* helper to partition the specified inputs into locally available and
   remote inputs *)
let partition_inputs taskinfo input_reqs =
  List.partition
    (function Local _ -> true | Remote _ -> false)
    (List.map (try_localize taskinfo) input_reqs)

(* download (if needed) data at specified locations and return open
   file handles to their local copies *)

type 'a input_resp = P.input_id * 'a * (Errors.error, (Uri.t * File.t)) U.lr

let inputs_from taskinfo (type cid_type) input_reqs =
  let locals, remotes = partition_inputs taskinfo input_reqs in
  (* open files for local data *)
  let local_results = List.map
      (function
        | Local (iid, cid, uri, fn) ->
            let url = Uri.to_string uri in
            U.dbg "Mapped loc %s of input %d to local file %s" url iid fn;
            (try iid, cid, U.Right (uri, File.open_existing fn)
             with Unix.Unix_error (ec, efn, es) ->
               U.dbg " error opening %s: %s (%s %s)"
                 fn (Unix.error_message ec) efn es;
               iid, cid, U.Left (E.Input_local_failure (url, ec)))
        | Remote _ ->
            assert false
      ) locals in
  (* retrieve remote data over HTTP into local files *)
  let module HC = C.Make(struct type t = P.input_id * cid_type * File.t end) in
  let make_req iid cid replicas =
    let f = (File.open_new ~delete_on_close:true
               (local_filename taskinfo (List.hd replicas))) in
    U.dbg "Attempting download to %s of [%s]"
      (File.name f) (Bytes.concat " " (List.map Uri.to_string replicas));
    Http.Get,
    C.FileRecv ((List.map Uri.to_string replicas), f.File.fd),
    (iid, cid, f) in
  let reqs = List.map
      (function
        | Remote (iid, cid, replicas) ->
            make_req iid cid replicas
        | Local _ ->
            assert false
      ) remotes in
  let remote_results = List.map
      (fun resp ->
        match resp with
        | {HC.request_id = (iid, cid, f); response = C.Failure (e, rest)} ->
            File.close f;
            iid, cid, U.Left (E.Input_failure (e :: rest))
        | {HC.request_id = (iid, cid, f); response = C.Success (r, _); url} ->
            let status = Http.Response.status_code r in
            if status = 200
            then iid, cid, U.Right ((Uri.of_string url), f)
            else begin
              File.close f;
              iid, cid, U.Left (E.Input_remote_failure (url, status))
            end
      ) (HC.request reqs) in
  (local_results @ remote_results)

(* a similar function to the above, which directly returns the data
   contents at the specified locations and does not create local files
   to copy remote data.  similar, but different enough that the common
   portions cannot be neatly extracted due to typing issues.  *)

type 'a payload_resp =
    Protocol.input_id * 'a * (Errors.error, (Uri.t * string)) Utils.lr

let payloads_from taskinfo (type cid_type) input_reqs =
  let locals, remotes = partition_inputs taskinfo input_reqs in
  (* read contents of local files *)
  let local_results = List.map
      (function
        | Local (iid, cid, uri, fn) ->
            let url = Uri.to_string uri in
            U.dbg "Mapped loc %s of input %d to local file %s" url iid fn;
            (try iid, cid, U.Right (uri, content_of fn)
             with Unix.Unix_error (ec, efn, es) ->
               U.dbg " error opening %s: %s (%s %s)"
                 fn (Unix.error_message ec) efn es;
               iid, cid, U.Left (E.Input_local_failure (url, ec)))
        | Remote _ ->
            assert false
      ) locals in
  (* retrieve remote payload *)
  let module HC = C.Make(struct type t = P.input_id * cid_type end) in
  let make_req iid cid replicas =
    U.dbg "Attempting retrieval of [%s]"
      (Bytes.concat " " (List.map Uri.to_string replicas));
    Http.Get, C.Payload ((List.map Uri.to_string replicas), None), (iid, cid) in
  let reqs = List.map
      (function
        | Remote (iid, cid, replicas) ->
            make_req iid cid replicas
        | Local _ ->
            assert false
      ) remotes in
  let remote_results = List.map
      (fun resp ->
        match resp with
        | {HC.request_id = (iid, cid); response = C.Failure (e, rest)} ->
            iid, cid, U.Left (E.Input_failure (e :: rest))
        | {HC.request_id = (iid, cid); response = C.Success (r, _); url} ->
            let status = Http.Response.status_code r in
            if status = 200
            then begin
              let buf = U.unopt (Http.Response.payload_buf r) in
              let content = Buffer.contents buf in
              let payload =
                if is_gzipped url then gunzip_str content url else content
              in iid, cid, U.Right ((Uri.of_string url), payload)
            end else
              iid, cid, U.Left (E.Input_remote_failure (url, status))
      ) (HC.request reqs) in
  local_results @ remote_results

(* open a task output file *)

let filename taskinfo name =
  Filename.concat taskinfo.P.task_rootpath name
let open_output_file taskinfo label =
  let fname = filename taskinfo (Printf.sprintf "part-disco-%09d" label)
  in File.open_new ~delete_on_close:false fname

(* client-side environment *)

let default_master_host () =
  try Unix.getenv "DISCO_MASTER_HOST"
  with Not_found -> Unix.gethostname ()

let default_port () =
  let sport = try Unix.getenv "DISCO_PORT" with Not_found -> "8989" in
  try int_of_string sport
  with Failure _ -> raise (E.Worker_failure (E.Invalid_port sport))
