val comparable_task_inputs : Protocol.replica_id * Pipeline.task_input
  -> Protocol.replica_id * Pipeline.task_input -> bool

val task_input_of : Protocol.taskinfo -> Protocol.input_id
  -> Protocol.replica list -> (Protocol.replica_id * Pipeline.task_input) list
