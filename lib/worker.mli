(** This module provides the entry point into ODisco. *)

val start : (module Task.TASK) -> unit
  (** The main Disco worker program should call [start] after
      performing any needed global initialization.  Once this is
      called, ODisco takes control and communicates with the worker
      only via the callbacks in the provided {!Task.TASK} module
      argument. *)
