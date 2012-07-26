(* Configuration of the Disco/DDFS master. *)

type config = {
  cfg_master : string;      (** the hostname of the master *)
  cfg_port   : int;         (** the Disco/DDFS port, usually 8989 *)
}

val safe_config : config option -> config
