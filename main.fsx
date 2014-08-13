#r "/home/ack/src/melin/fs/lib/JSONNet/Newtonsoft.Json.dll"

open System
open Newtonsoft.Json

// Identifier type
type Id = System.Guid

module EventStore =
    type VersionedEvent = int * string
    type PersistableTransition = (Id * string[])
    let store = new System.Collections.Generic.Dictionary<Id,VersionedEvent[]>()

    // Event serializer
    let toPersistable<'a, 'b> (id: Id, transition: ('a[] * 'b)) =
        let events, _ = transition
        (id, events |> Array.map (fun event-> JsonConvert.SerializeObject(event)))

    let commit (transitions: PersistableTransition[]) =
        // Function to get next aggregate version
        let getMaxVersion id =
            if store.ContainsKey(id) then
                store.[id]
                 |> Array.map (fun (version, _) -> version)
                 |> Array.max
            else 0
        // Function to convert list of events to list of versioned events
        let toVersionedEvents (id, events) =
            events |> Array.map (fun event -> ((id |> getMaxVersion)+1, event))
        // For each transition either append versioned events to existing or add new
        transitions
         |> Array.map (fun (id, events) -> 
            if store.ContainsKey(id) then store.[id] <- Array.append store.[id] ((id, events) |> toVersionedEvents)
            else store.Add(id, ((id, events) |> toVersionedEvents)))

    let get<'a, 'b> (id: Id, initial: 'b, apply) =
        // Get events for aggregate, deserialize and replay
        let events = 
            store.[id]
             |> Array.map (fun (_, event) -> event)
             |> Array.map (fun itm ->JsonConvert.DeserializeObject<'a>(itm))
        events
         |> Seq.fold apply initial
'
// All of task
module Task =
    type TaskInfo = {
        Name: string;
        Description: string;
        Duedate: System.DateTime;
        Priority: int;
    }

    // Internal task commands
    type Command =
        | New of id: Id * info: TaskInfo
        | Rename of name: string
        | Assign of Id

    // Task events
    type Event =
        | TaskCreated of TaskInfo
        | TaskRenamed of name: string
        | TaskAssigned of Id

    // Task aggregate
    type State = { Info: TaskInfo; AssignedTo: Id; }
    with static member Initial = { Info = { Name = ""; Description = ""; Duedate = DateTime.Now; Priority = 0; }; AssignedTo = System.Guid.NewGuid(); }

    // Represents a task state transition, appends events and keeps state
    type Transition = Event[] * State

    // Applies events to aggregate building current state
    let apply item = function
        | TaskCreated event -> { item with State.Info = event }
        | TaskRenamed event -> { item with State.Info = { item.Info with Name = event } }
        | TaskAssigned event -> { item with AssignedTo = event }

    // Assertions for validating commands
    module private Assert =
        let validName name = if System.String.IsNullOrEmpty(name) then invalidArg "name" "The name must not be null." else name
        let assignable id = if id.Equals(null) then invalidArg "id" "Cannot assign task to null." else id

    // Handles internal task commands
    let exec cmd transition = 
        let originalEvents, item = transition
        // Create function to apply new events and generate next transition
        let apply events = (Array.append originalEvents events, Seq.fold apply item events)
        // Handle command
        match cmd with
            | New(id, info) -> info |> fun info -> [| info |> TaskCreated |] |> apply
            | Rename(name) -> name |> Assert.validName |> fun name -> [| name |> TaskRenamed |] |> apply
            | Assign(id) -> id |> Assert.assignable |> fun id -> [| id |> TaskAssigned |] |> apply

    // Reads aggregate from events store and creates transition from current state
    let read id =
        ([||], (id, State.Initial, apply) |> EventStore.get<Event,State>): Transition

    // Serializes transition to persistable events
    let toPersistable id transition = 
        [| (id, transition) |> EventStore.toPersistable |]

    // Handles external new task command
    let newTask (id: Id, info: TaskInfo, assignedTo: Id) =
        let task: Transition = (Array.empty, State.Initial)
        task
         |> exec ((id, info) |> New)
         |> exec (assignedTo |> Assign)
         |> toPersistable id
         |> EventStore.commit

    // Handles external rename task command
    let rename (id: Id, name: string) =
        read id
         |> exec (name |> Rename)
         |> toPersistable id
         |> EventStore.commit

// External commands
type Command =
    | NewTask of Id * Task.TaskInfo * Id
    | RenameTask of Id * string

// Command handler
let handle cmd =
    match cmd with
     | NewTask(id, info, assignedTo) -> (id, info, assignedTo) |> Task.newTask
     | RenameTask(id, name) -> (id, name) |> Task.rename


// ############################3 Testing it ############################################################
// Create a new task and then rename it
let id = System.Guid.NewGuid()
let assignee = System.Guid.NewGuid()
NewTask(id, {Name = "A new task"; Description = "This is a new task"; Duedate = DateTime.Now; Priority = 0; }, assignee) |> handle
RenameTask(id, "This is a renamed task") |> handle

// Output task from the event store
Task.read id

// Output all tasks from the event store
EventStore.store