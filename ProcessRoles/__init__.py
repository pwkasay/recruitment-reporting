from ProcessRoles.main import *
import logging
import azure.functions as func


def main(req: func.HttpRequest) -> func.HttpResponse:
    logging.info("Python HTTP trigger function processed a request.")
    print("Main triggered")
    try:
        result = process()
        # return func.HttpResponse(result, status_code=200)
        if result.status_code == 200:
            return func.HttpResponse(
                f"Main 1 - Processed - {result.status_code}", status_code=200
            )
        elif result.status_code == 500:
            return func.HttpResponse(
                f"Main 0 Failed to Process - {result.get_body()} -{result.status_code}",
                status_code=500,
            )
        else:
            return func.HttpResponse("Main 0 Failed to Process - ")
    except Exception as e:
        logging.error(f"Main exception found: {e}")
        return func.HttpResponse(str(e), status_code=500)
