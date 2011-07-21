(** The interface to the computation performed by an ODisco worker is
    described by this module. *)

(** The ODisco library implements the Disco worker protocol, through
    which the inputs to and outputs from a Disco task are
    communicated.  An ODisco worker implements the computation on
    those inputs, and generates the outputs for the task.

    For more detailed information, please see the @see
    <http://discoproject.org/doc/howto/worker.html> Disco worker
    protocol.

    To first define some terminology: the term {i ODisco worker} will
    denote the whole OCaml program that links to the ODisco library;
    this would be like a Unix program.  The term {i Disco task}, on
    the other hand, will denote a specific running instance of the
    ODisco worker, that performs a specific computation (e.g. a map
    task or a reduce task).  The Unix counterpart is a process.  A
    collection of Disco map and reduce tasks implement a {i Disco
    job}.

    Once ODisco is given control (via {!Worker.start}), it initiates
    the Disco worker protocol to Disco, and retrieves the inputs
    one-by-one for the Disco task.  Before the first input is passed
    to the ODisco worker, the appropriate init callback for the task
    is called, i.e. either [map_init] or [reduce_init].  Then, after
    each input is retrieved, the appropiate processing function for
    the task is called, i.e. either [map] or [reduce].  After the last
    input has been processed, the appropriate finalize callback for
    the task is called, i.e. either [map_done] or [reduce_done].
    After the finalize callback returns to ODisco, ODisco terminates
    the worker protocol, and ends the task.

    Each callback receives a [disco] argument, which will contain some
    information for the current task, as well as functions through
    which the worker can write some task output or log some messages
    to Disco.

    When generating output, the worker can choose to optionally label
    the output channel with an integer label; this can be used to
    implement partitioned output.  Please see the Disco documentation
    on how partitioning is used in @see
    <http://discoproject.org/doc/howto/dataflow.html> the Disco
    processing pipeline.

    If any exceptions are thrown in the worker code, ODisco catches
    the exception, and generates the appropriate error message to
    Disco, and terminates the task with an error. *)

(** The argument to every callback in the {!Task.TASK} implementation,
    containing task information as well functions to generate output
    or log messages. *)
type disco = {
  taskname : string;    (** the name of the task *)
  hostname : string;    (** the host on which the task is running *)

  input_url : string;   (** the url corresponding to the current input *)
  input_size : int;     (** the size of the current input in bytes *)

  out_channel : label:int option -> out_channel; (** the output channel selector *)
  log : string -> unit; (** a function to log a message to Disco *)

  temp_dir : string; (** a directory in which to create temporary files *)
}

(** This signature specifies the callbacks that ODisco invokes for the
    worker to perform the task-specific computation. *)
module type TASK = sig

  (** This type specifies the return value of the [map_init] function.
      The return value of [map_init] will be provided to the other
      [map_] callbacks. *)
  type map_init

  (** This function is the first invoked callback for a map task.
      When [map_init disco] is invoked, the [disco.url] field is set
      to the empty string, and [disco.input_size] is set to 0. *)
  val map_init : disco -> map_init

  (** The primary map processing function.  This function is called
      once for each input to the task.  When [map_init disco
      in_channel] is invoked, the [in_channel] is opened to the
      beginning of the input. *)
  val map : map_init -> disco -> in_channel -> unit

  (** This function is called once after all [map] inputs have been
      processed.  This is the last callback invoked in a map task. *)
  val map_done : map_init -> disco -> unit

  (** The following types and values behave exactly like those above,
      but for reduce tasks. *)

  type reduce_init
  val reduce_init : disco -> reduce_init
  val reduce : reduce_init -> disco -> in_channel -> unit
  val reduce_done : reduce_init -> disco -> unit
end
