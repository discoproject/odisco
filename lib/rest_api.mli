type req = Http_client.meth * Http_client.request * Http_client.request_id

val url_for_job_submit : Cfg.config -> string

val url_for_tagname : Cfg.config -> string -> string

val url_for_tag_list : Cfg.config -> string

val payload_of_req : ?timeout:float -> req -> (Http_client.error -> 'a)
  -> ('a, string) Utils.lr
