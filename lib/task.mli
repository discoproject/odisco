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
    collection of Disco tasks implement a {i Disco job}.

    Once ODisco is given control (via {!Worker.start}), it initiates
    the Disco worker protocol to Disco, and retrieves the inputs
    one-by-one for the Disco task.  Before the first input is passed
    to the ODisco worker, the [task_init] callback for the task is
    called.  Then, after each input is retrieved, the [process]
    function for the task is called.  After the last input has been
    processed, the [task_done] callback for the task is called.  After
    the done callback returns to ODisco, ODisco terminates the worker
    protocol, and ends the task.

    Each callback receives a [disco] argument, which will contain some
    information for the current task, as well as functions through
    which the worker can write some task output or log some messages
    to Disco.

    When generating output, the worker specifies an integer label for
    each output channel.

    If any exceptions are thrown in the worker code, ODisco catches
    the exception, and generates the appropriate error message to
    Disco, and terminates the task with an error. *)


(** Each task is in a pipeline stage. *)
type stage = Pipeline.stage

(** Each stage performs a user-specified grouping. *)
type grouping = Pipeline.grouping

(** Each task input has a label. *)
type label = Pipeline.label

(** The argument to every callback in the {!Task.TASK} implementation,
    containing task information as well functions to generate output
    or log messages. *)
type disco = {
  (* The following are the same for each task in the job. *)
  jobname : string;     (** the name of the job *)
  jobfile : string;     (** the relative path where the jobfile can be found *)

  (* The following are particular to each task, but remain the same
     across all executions of the same task. *)
  stage : stage;              (** the stage of the current task *)
  grouping : grouping;        (** the grouping performed by the stage *)
  group_label : label;        (** the group label of the current task *)
  group_node : string option; (** the group node of the current task *)
  task_id  : int;             (** the task id of the current task *)

  (* The following may change across executions of same task, e.g. due
     to node failures, the same task may be re-executed on different
     hosts. *)
  hostname : string;    (** the host on which the task is running *)

  (* The following specify information about the current input for the
     task. *)
  input_label : label;  (** the label for the current input *)
  input_url : string;   (** the url corresponding to the current input *)
  input_path : string;  (** the relative path of the current input file *)
  input_size : int;     (** the size of the current input in bytes *)

  (* The following specify output and log message functions for the task. *)
  out_channel : label:label -> out_channel; (** the output channel selector *)
  log : string -> unit;                   (** log a message to Disco *)

  temp_dir : string;    (** a directory in which to create temporary files *)
}

(** This signature specifies the callbacks that ODisco invokes for the
    worker to perform the task-specific computation. *)
module type TASK = sig

  (** This type specifies the return value of the [task_init] function.
      The return value of [task_init] will be provided to the other
      [task_] callbacks. *)
  type init

  (** This function is the first invoked callback for a task.  When
      [task_init disco] is invoked, the [disco.input_url]
      [disco.input_path] fields are set to the empty string, and
      [disco.input_size] is set to 0. *)
  val task_init : disco -> init

  (** The primary processing function.  This function is called once
      for each input to the task.  When [task_process disco
      in_channel] is invoked, the [in_channel] is opened to the
      beginning of the input. *)
  val task_process : init -> disco -> in_channel -> unit

  (** This function is called once after all inputs have been
      processed.  This is the last callback invoked in a task. *)
  val task_done : init -> disco -> unit

end
