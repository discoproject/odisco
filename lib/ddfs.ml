(* DDFS API *)

module JC = Json_conv
module JP = Json_parse
module H = Http
module J = Json
module E = Errors
module N = Env
module U = Utils
module C = Http_client
module Api = Rest_api

module StringSet = Set.Make (struct type t = string let compare = compare end)
module HC = Http_client.Make(struct type t = unit end)

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

let client_norm_uri cfg uri =
  let trans_auth =
    match uri.Uri.authority with
    | None -> None
    | Some a -> Some {a with Uri.port = Some cfg.Cfg.cfg_port}
  in match uri.Uri.scheme with
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

let tag_payload_of_name ?cfg ?timeout tag_name =
  let url = Api.url_for_tagname (Cfg.safe_config cfg) tag_name in
  let err_of e = E.Tag_retrieval_failure (tag_name, e) in
  Api.payload_of_req ?timeout (H.Get, C.Payload ([url], None)) err_of

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
    (* Use List.rev_map to handle tags with a very large number of blobsets. *)
    let tag_urls = List.rev
        (List.rev_map
           (fun bs ->
             List.map (Uri.of_string @@ JC.to_string) (JC.to_list bs)
           )
           ju) in
    let jattribs = JC.to_object_table (JC.object_field o "user-data") in
    let tag_attribs = (List.map (fun (k, v)  -> k, JC.to_string v)
                         (JC.object_table_to_list jattribs)) in
    U.Right {tag_id; tag_last_modified; tag_attribs; tag_urls}
  with
  | JC.Json_conv_error e -> U.Left (E.Unexpected_json e)
  | Uri.Uri_error e -> U.Left (E.Invalid_uri e)

let (+>) = U.(+>)

let tag_of_tagname ?cfg ?timeout n =
  (tag_payload_of_name ?cfg ?timeout n) +> tag_json_of_payload +> tag_of_json

let child_tags_of_tag t =
  let children = List.fold_left
      (fun s bs ->
        List.fold_left
          (fun s u ->
            if is_tag_url u then StringSet.add (tag_name_of_url u) s else s
          ) s bs
      ) StringSet.empty t.tag_urls in
  U.Right (StringSet.elements children)

let child_tags_of_tag_name ?cfg ?timeout n =
  (tag_of_tagname ?cfg ?timeout n) +> child_tags_of_tag

let blob_size ?cfg ?timeout blobset =
  let cfg = Cfg.safe_config cfg in
  let urls = List.map (Uri.to_string @@ (client_norm_uri cfg)) blobset in
  match HC.request ?timeout [(H.Head, C.Payload (urls, None), ())] with
  | {HC.response = C.Success (r, _)} :: _ ->
      (try
        let len_hdr = H.lookup_header "Content-Length" (H.Response.headers r) in
        let len_str = (String.concat ", " len_hdr) in
        Some (int_of_string len_str)
      with
      | Failure _ ->
          U.dbg "Error converting content-length for: %s" (String.concat ", " urls);
          None
      | Not_found ->
          U.dbg "No content-length found in: %s" (String.concat ", " urls);
          None)
  | {HC.response = C.Failure ((u, e), uel)} :: _ ->
      List.iter
        (fun (u, e) ->
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

let tag_list ?cfg ?timeout () =
  let url = Api.url_for_tag_list (Cfg.safe_config cfg) in
  let err_of e = E.Tag_list_failure e in
  ((Api.payload_of_req ?timeout (H.Get, C.Payload ([url], None)) err_of)
     +> tag_list_of_payload +> tag_list_of_json)
