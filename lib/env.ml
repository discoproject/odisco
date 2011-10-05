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
    in { name; fd; delete_on_close }

  let open_existing name =
    let flags = [Unix.O_RDONLY] in
    let fd = Unix.openfile name flags 0o640
    in { name; fd; delete_on_close = false }

  let close f =
    (try Unix.close f.fd
     with Unix.Unix_error (Unix.EBADF, _, _) -> ());
    if f.delete_on_close then Unix.unlink f.name
end

(* internal decompression utilities *)

let is_gzipped url =
  let len = String.length url in
  if len < 3 (* ".gz" *) then false
  else String.sub url (len - 3) 3 = ".gz"

let filesize fname = (Unix.stat fname).Unix.st_size

let contents_of_gzip fname =
  let zsz = filesize fname in
  let inc = Gzip.open_in fname in
  let buf = Buffer.create zsz in
  let sbuf = String.create zsz in
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
  contents_of_gzip tname

(* internal utilities to handle URLs pointing inside local Disco filesystem *)

let dISCO_PATH = "/disco/"
let dDFS_PATH  = "/ddfs/"

let is_known_path = function
  | None -> false
  | Some p -> U.is_prefix p dISCO_PATH || U.is_prefix p dDFS_PATH

let find_local_file taskinfo replicas =
  let localhost = taskinfo.P.task_host in
  let locals = List.filter
    (fun u ->
      ((match u.Uri.authority with
        | None -> false
        | Some a -> a.Uri.host = localhost
       ) && (is_known_path u.Uri.path))
      || u.Uri.scheme = Some "file"
    ) replicas in
  match locals with
    | [] -> None
    | l :: _ -> Some l

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

type input_location =
  | Remote of Http_client.request_id * Uri.t list
  | Local of Http_client.request_id * Uri.t * (* filename *) string

let try_localize taskinfo (request_id, replicas) =
  match (find_local_file taskinfo replicas) with
    | None -> Remote (request_id, replicas)
    | Some l -> Local (request_id, l, local_file_of taskinfo l)

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

type input_req = Http_client.request_id * Uri.t list

(* download files at specified locations and return open file handles
   to their local copies *)

type input_resp = Http_client.request_id * (Errors.error, (Uri.t * File.t)) U.lr

let inputs_from taskinfo input_reqs =
  let locals, remotes = List.partition
    (function Local _ -> true | Remote _ -> false)
    (List.map (try_localize taskinfo) input_reqs) in
  (* local data *)
  let local_results = List.map
    (function
      | Local (id, uri, fn) ->
        U.dbg "Mapped %s to local file %s" (Uri.to_string uri) fn;
        id, U.Right (uri, File.open_existing fn)
      | Remote _ -> assert false
    ) locals in
  (* retrieve remote data over HTTP *)
  let req_id = ref 0 in
  let make_req id replicas =
    incr req_id;
    let f = (File.open_new ~delete_on_close:true
               (local_filename taskinfo (List.hd replicas))) in
    U.dbg "Attempting download to %s of [%s]"
      (File.name f) (String.concat " " (List.map Uri.to_string replicas));
    (!req_id, ((f, id), (Http.Get,
                         C.FileRecv ((List.map Uri.to_string replicas), f.File.fd),
                         !req_id))) in
  let req_map = List.map
    (function
      | Remote (id, replicas) -> make_req id replicas
      | Local _ -> assert false
    ) remotes in
  let remote_results = List.map
    (fun resp ->
      match resp with
        | { C.response = None; error = None } ->
          assert false
        | { C.request_id = rq_id; response = None; error = Some e } ->
          let f, id = fst (List.assoc rq_id req_map) in
          File.close f;
          id, U.Left (E.Input_failure e)
        | { C.request_id = rq_id; C.response = Some r; C.url = url } ->
          let f, id = fst (List.assoc rq_id req_map) in
          let status = Http.Response.status_code r in
          if status = 200
          then id, U.Right ((Uri.of_string url), f)
          else id, U.Left (E.Input_response_failure (url, status))
    ) (C.request (List.map (fun e -> snd (snd e)) req_map)) in
  local_results @ remote_results

(* retrieve uncompressed payloads from specified locations *)

type payload_resp = Http_client.request_id * (Errors.error, (Uri.t * string)) U.lr

let payloads_from taskinfo input_reqs =
  let locals, remotes = List.partition
    (function Local _ -> true | Remote _ -> false)
    (List.map (try_localize taskinfo) input_reqs) in
  (* local data *)
  let local_results = List.map
    (function
      | Local (id, uri, fn) ->
        id, U.Right (uri, contents_of_gzip fn)
      | Remote _ -> assert false
    ) locals in
  (* retrieve remote data over HTTP *)
  let make_req id replicas =
    (id, (Http.Get, C.Payload (List.map Uri.to_string replicas, None), id)) in
  let req_map = List.map
    (function
      | Remote (id, replicas) -> make_req id replicas
      | Local _ -> assert false
    ) remotes in
  let remote_results = List.map
    (fun resp ->
      match resp with
        | { C.response = None; error = None } ->
          assert false
        | { C.request_id = id; response = None; error = Some e } ->
          id, U.Left (E.Input_failure e)
        | { C.request_id = id; C.response = Some r; C.url = url } ->
          let status = Http.Response.status_code r in
          if status <> 200
          then id, U.Left (E.Input_response_failure (url, status))
          else
            let buf = U.unopt (Http.Response.payload_buf r) in
            let content = Buffer.contents buf in
            let payload = if is_gzipped url then gunzip_str content url else content in
            id, U.Right ((Uri.of_string url), payload)
    ) (C.request (List.map snd req_map)) in
  local_results @ remote_results

(* parse an index payload *)

let parse_index s =
  let parse_line l =
    match U.string_split l ' ' with
      | p :: url :: [] -> p, url
      | _ -> assert false
  in List.map parse_line (U.string_split s '\n')

(* open a task output file *)

let filename taskinfo name =
  Filename.concat taskinfo.P.task_rootpath name
let open_output_file taskinfo label =
  let stage = P.string_of_stage taskinfo.P.task_stage in
  let fname =
    filename taskinfo
      (match label with
        | None -> Printf.sprintf "%s-disco-%d-%09d" stage taskinfo.P.task_id 0
        | Some l -> Printf.sprintf "part-disco-%09d" l)
  in File.open_new ~delete_on_close:false fname
