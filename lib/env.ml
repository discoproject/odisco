module P = Protocol
module E = Errors
module C = Http_client
module U = Utils

module File = struct
  type t = {
    name : string;
    fd   : Unix.file_descr;
    delete_on_close : bool;
  }
  let name f = f.name
  let fd f = f.fd

  let open_new ~delete_on_close name =
    U.dbg "opening new file %s (%s)"
      name (if delete_on_close then "delete_on_close" else "keep_on_close");
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
    if f.delete_on_close then begin
      U.dbg "deleting file %s" f.name;
      Unix.unlink f.name
    end
end

let filename taskinfo name =
  Filename.concat taskinfo.P.task_rootpath name

let local_filename taskinfo url_path =
  Filename.concat taskinfo.P.task_rootpath (Filename.basename url_path)

let download_urls replicas f =
  U.dbg "Downloading %s" (String.concat " " replicas);
  match C.request [(Http.Request_header.Get, C.FileRecv (replicas, f.File.fd))] with
    | [] | _::_::_
    | [{ C.response = None; C.error = None }] ->
        assert false
    | [{ C.response = None; C.error = Some e }] ->
        File.close f;
        Some (E.Input_failure e)
    | [{ C.response = Some resp; C.url = url }] ->
        let status = resp.Http.Response.response.Http.Response_header.status_code in
          if status = 200 then None
          else begin
            File.close f;
            Some (E.Input_response_failure (url, status))
          end

type download_result =
  | Download_file of File.t
  | Download_error of E.error

let download replicas taskinfo =
  match replicas with
    | [] ->
        assert false
    | uri :: _ ->
        (match uri.Uri.scheme, uri.Uri.path with
           | Some "file", Some p ->
               Download_file (File.open_existing p)
           | Some "http", Some p ->
               let f = File.open_new ~delete_on_close:true (local_filename taskinfo p)
               in (match download_urls (List.map Uri.to_string replicas) f with
                     | Some err -> Download_error err
                     | None -> Download_file f)
           | _ ->
               Download_error (E.Invalid_input (Uri.to_string uri)))

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
      end
  in
    slurp ();
    Gzip.close_in inc;
    Buffer.contents buf

let gunzip_str s url =
  let (tname, toc) =
    Filename.open_temp_file ~temp_dir:"/tmp" "tmp-" (Filename.basename url) in
    output_string toc s;
    close_out toc;
    contents_of_gzip tname

let get_payload urls =
  let url = List.hd urls in
  let is_gzip = is_gzipped url in
  let unzip s = if is_gzip then gunzip_str s url else s in
  match C.request [Http.Request_header.Get, C.Payload (urls, None)] with
    | [] | _::_::_
    | [{ C.response = None; C.error = None }] ->
        assert false
    | [{ C.response = None; C.error = Some e }] ->
        raise (E.Worker_failure (E.Input_failure e))
    | [{ C.response = Some resp; C.url = url }] ->
        let status = resp.Http.Response.response.Http.Response_header.status_code in
          if status = 200 then begin
            match resp.Http.Response.payload with
              | None -> assert false
              | Some p -> unzip (Buffer.contents p.Http.Payload.content)
          end else raise (E.Worker_failure (E.Input_response_failure (url, status)))

let open_output_file taskinfo label =
  let stage = P.string_of_stage taskinfo.P.task_stage in
  let fname = (filename taskinfo
                 (match label with
                    | None -> Printf.sprintf "%s-disco-%d-%09d" stage taskinfo.P.task_id 0
                    | Some l -> Printf.sprintf "part-disco-%09d" l))
  in File.open_new ~delete_on_close:false fname

let open_map_index taskinfo =
  let fname = filename taskinfo "map-index.txt"
  in File.open_new ~delete_on_close:false fname

let index_file taskinfo part_fname =
  Filename.concat taskinfo.P.task_rootpath (Filename.basename part_fname)

let write_index taskinfo partinfo =
  let f = open_map_index taskinfo in
  let oc = Unix.out_channel_of_descr f.File.fd in
    List.iter (fun (p, fn) ->
                 output_string oc (Printf.sprintf "%d %s\n" p fn)
              ) partinfo;
    close_out oc

let parse_index s =
  let parse_line l =
    match U.string_split l ' ' with
      | p :: url :: [] -> p, url
      | _ -> assert false
  in List.map parse_line (U.string_split s '\n')

let filter_index part indx =
  List.filter (fun (i, _) -> i = part) indx

let read_index _taskinfo urls =
  parse_index (get_payload (List.map Uri.to_string urls))

let filter_index taskinfo part_urls =
  let pi = read_index taskinfo part_urls in
    List.map snd (filter_index (string_of_int taskinfo.P.task_id) pi)

let open_reduce_output taskinfo =
  let fname = filename taskinfo (Printf.sprintf "reduce-disco-%d" taskinfo.P.task_id)
  in File.open_new ~delete_on_close:false fname
