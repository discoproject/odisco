(** This module provides the entry point into ODisco. *)

val start : (Pipeline.stage * (module Task.TASK)) list -> unit
(** The main Disco worker program should call [start] after performing
    any needed global initialization.  Once this is called, ODisco
    takes control and communicates with the worker only via the
    callbacks in the {!Task.TASK} module associated with the
    appropriate stage. *)
