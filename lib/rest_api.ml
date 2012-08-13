module H = Http
module C = Http_client
module U = Utils
module HC = Http_client.Make(struct type t = unit end)

type req = H.meth * C.request

let url_for_job_submit cfg =
  Printf.sprintf "http://%s:%d/disco/job/new"
    cfg.Cfg.cfg_master cfg.Cfg.cfg_port

let url_for_tagname cfg tag_name =
  Printf.sprintf "http://%s:%d/ddfs/tag/%s"
    cfg.Cfg.cfg_master cfg.Cfg.cfg_port tag_name

let url_for_tag_list cfg =
  Printf.sprintf "http://%s:%d/ddfs/tags"
    cfg.Cfg.cfg_master cfg.Cfg.cfg_port

let payload_of_req ?timeout (meth, req)  err_of =
  match HC.request ?timeout [(meth, req, ())] with
  | {HC.response = C.Success (r, _)} :: _ ->
      U.Right (Buffer.contents (U.unopt (H.Response.payload_buf r)))
  | {HC.response = C.Failure ((_url, e), _)} :: _ ->
      U.Left (err_of e)
  | [] -> assert false
