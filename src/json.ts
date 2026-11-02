import { SerializationError } from "./errors.js";

export const toJson = (value: unknown, what: string): string => {
    try {
        return JSON.stringify(value) ?? "null";
    } catch (e) {
        throw new SerializationError(`${what} is not JSON-serializable`, { cause: e });
    }
};