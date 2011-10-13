(* DDFS API *)

module H = Http
module C = Http_client
module JC = Json_conv
module JP = Json_parse
module J = Json
module E = Errors
module N = Env
module U = Utils

(* Configuration of the DDFS master. *)
type config = {
  cfg_master : string;      (** the hostname of the DDFS master *)
  cfg_port   : int;         (** the DDFS port, usually the same as the Disco port, i.e. 8989 *)
}

(* DDFS types *)

type tag_name = string
type token    = string
type attrib   = string * string

type blob    = Uri.t
type blobset = blob list

type tag = {
  tag_id : string;
  tag_last_modified : string;
  tag_attribs : attrib list;
  tag_urls : blobset list;
}

let safe_config = function
  | Some c -> c
  | None ->
    {cfg_master = N.default_master_host (); cfg_port = N.default_port ()}

let url_for_tagname cfg tag_name =
  Printf.sprintf "http://%s:%d/ddfs/tag/%s" cfg.cfg_master cfg.cfg_port tag_name

let url_for_tag_list cfg =
  Printf.sprintf "http://%s:%d/ddfs/tags" cfg.cfg_master cfg.cfg_port

let client_norm_uri cfg uri =
  let trans_auth =
    match uri.Uri.authority with
      | None -> None
      | Some a -> Some {a with Uri.port = Some cfg.cfg_port}
  in
  match uri.Uri.scheme with
    | None ->
      {uri with Uri.scheme = Some "file"}
    | Some "dir" | Some "disco" ->
      {uri with Uri.scheme = Some "http"; authority = trans_auth}
    | Some _ ->
      uri

let is_tag_url u =
  match u.Uri.scheme with
    | Some "tag" -> true
    | Some _ | None -> false

let tag_name_of_url u =
  let auth = U.unopt u.Uri.authority in
  match auth with
    | {Uri.userinfo = None;   Uri.port = None}   -> auth.Uri.host
    | {Uri.userinfo = None;   Uri.port = Some p} -> Printf.sprintf "%s:%d" auth.Uri.host p
    | {Uri.userinfo = Some u; Uri.port = None}   -> Printf.sprintf "%s@%s" u auth.Uri.host
    | {Uri.userinfo = Some u; Uri.port = Some p} -> Printf.sprintf "%s@%s:%d" u auth.Uri.host p

let payload_of_req req err_of =
  match C.request [req] with
    | {C.response = C.Success (r, _)} :: _ ->
      U.Right (Buffer.contents (U.unopt (H.Response.payload_buf r)))
    | {C.response = C.Failure ((_url, e), _)} :: _ ->
      U.Left (err_of e)
    | [] -> assert false

let tag_payload_of_name ?cfg tag_name =
  let url = url_for_tagname (safe_config cfg) tag_name in
  let err_of e = E.Tag_retrieval_failure (tag_name, e) in
  payload_of_req (H.Get, C.Payload ([url], None), 0) err_of

let tag_json_of_payload p =
  try U.Right (JP.of_string p)
  with JP.Parse_error e -> U.Left (E.Invalid_json e)

let (@@) f g = fun x -> f (g x)

let tag_of_json j =
  try
    let o = JC.to_object_table j in
    let tag_id = JC.to_string (JC.object_field o "id") in
    let tag_last_modified = JC.to_string (JC.object_field o "last-modified") in
    let ju = JC.to_list (JC.object_field o "urls") in
    let tag_urls = (List.map
                      (fun bs ->
                        List.map (Uri.of_string @@ JC.to_string) (JC.to_list bs))
                      ju) in
    let jattribs = JC.to_object_table (JC.object_field o "user-data") in
    let tag_attribs = (List.map
                         (fun (k, v)  ->
                           k, JC.to_string v)
                         (JC.object_table_to_list jattribs)) in
    U.Right {tag_id; tag_last_modified; tag_attribs; tag_urls}
  with
    | JC.Json_conv_error e -> U.Left (E.Unexpected_json e)
    | Uri.Uri_error e -> U.Left (E.Invalid_uri e)

let (+>) = U.(+>)

let tag_of_tagname n =
  (tag_payload_of_name n) +> tag_json_of_payload +> tag_of_json

let blob_size ?cfg blobset =
  let cfg = safe_config cfg in
  let urls = List.map (Uri.to_string @@ (client_norm_uri cfg)) blobset in
  match C.request [(H.Head, C.Payload (urls, None), 0)] with
    | {C.response = C.Success (r, _)} :: _ ->
      (try
         let len_hdr = H.lookup_header "Content-Length" (H.Response.headers r) in
         let len_str = (String.concat ", " len_hdr) in
         Some (int_of_string len_str)
       with
         | Failure "int_of_string" ->
           U.dbg "Error converting content-length for: %s" (String.concat ", " urls);
           None
         | Not_found ->
           U.dbg "No content-length found in: %s" (String.concat ", " urls);
           None)
    | {C.response = C.Failure ((u, e), uel)} :: _ ->
      List.iter (fun (u, e) ->
        U.dbg "Retrieval of %s failed: %s" u (C.string_of_error e)
      ) ((u, e) :: uel);
      None
    | [] -> assert false

let tag_list_of_payload p =
  try U.Right (JP.of_string p)
  with JP.Parse_error e -> U.Left (E.Invalid_json e)

let tag_list_of_json j =
  try U.Right (List.map JC.to_string (JC.to_list j))
  with JC.Json_conv_error e -> U.Left (E.Unexpected_json e)

let tag_list ?cfg () =
  let url = url_for_tag_list (safe_config cfg) in
  let err_of e = E.Tag_list_failure e in
  ((payload_of_req (H.Get, C.Payload ([url], None), 0) err_of)
   +> tag_list_of_payload +> tag_list_of_json)
