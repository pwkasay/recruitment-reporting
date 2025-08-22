from main import *
import logging
import azure.functions as func


async def main(req: func.HttpRequest) -> func.HttpResponse:
    logging.info("Python HTTP trigger function processed a request.")
    print("Main triggered")

    created_after_date = (
        datetime.datetime.now(datetime.UTC) - datetime.timedelta(days=1)
    ).strftime("%Y-%m-%dT%H:%M:%SZ")

    created_before_date = (
        datetime.datetime.now(datetime.UTC) + datetime.timedelta(days=1)
    ).strftime("%Y-%m-%dT%H:%M:%SZ")


    try:
        result = await process(created_after_date, created_before_date)
        if result.status_code == 200:
            return func.HttpResponse(
                f"Main 1 - Processed - {result.status_code}", status_code=200
            )
        else:
            return func.HttpResponse(
                f"Main 0 Failed to Process - {result.get_body()} -{result.status_code}",
                status_code=500,
            )
    except Exception as e:
        logging.error(f"Main exception found: {e}")
        return func.HttpResponse(str(e), status_code=500)
