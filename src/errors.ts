export class DurableError extends Error {
    constructor(message: string, options?: ErrorOptions) {
        super(message, options);
        this.name = new.target.name
    }
}

export class DuplicateWorkflowError extends DurableError {}
export class SerializationError extends DurableError {}