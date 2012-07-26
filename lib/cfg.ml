module N = Env

type config = {
  cfg_master : string;      (** the hostname of the master *)
  cfg_port   : int;         (** the Disco/DDFS port, usually 8989 *)
}

let safe_config = function
  | Some c -> c
  | None ->
    {cfg_master = N.default_master_host (); cfg_port = N.default_port ()}
