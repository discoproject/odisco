type req = Http.meth * Http_client.request

val url_for_job_submit : Cfg.config -> string

val url_for_tagname : Cfg.config -> string -> string

val url_for_tag_list : Cfg.config -> string

val payload_of_req : ?timeout:float -> req -> (Http_client.error -> 'a)
  -> ('a, string) Utils.lr
