package at.uibk.dps.util;

/**
 * The log event.
 */
public enum Event {
    WORKFLOW_START,
    WORKFLOW_END,
    WORKFLOW_FAILED,
    FUNCTION_START,
    FUNCTION_END,
    FUNCTION_FAILED,
    FUNCTION_CANCELED,
    PARALLEL_FOR_END
}
