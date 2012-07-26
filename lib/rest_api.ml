module H = Http
module C = Http_client
module U = Utils

type req = C.meth * C.request * C.request_id

let url_for_tagname cfg tag_name =
  Printf.sprintf "http://%s:%d/ddfs/tag/%s"
    cfg.Cfg.cfg_master cfg.Cfg.cfg_port tag_name

let url_for_tag_list cfg =
  Printf.sprintf "http://%s:%d/ddfs/tags" cfg.Cfg.cfg_master cfg.Cfg.cfg_port

let payload_of_req ?timeout req err_of =
  match C.request ?timeout [req] with
    | {C.response = C.Success (r, _)} :: _ ->
      U.Right (Buffer.contents (U.unopt (H.Response.payload_buf r)))
    | {C.response = C.Failure ((_url, e), _)} :: _ ->
      U.Left (err_of e)
    | [] -> assert false
