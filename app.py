from flask import Flask, request, jsonify, Response, stream_with_context
import glob
import json
import os
import time

app = Flask(__name__)


@app.route("/files")
def get_file_names():
    """
    Gets file names from static directory
    :return: filenames as list inside dict under "filenames" key
    """
    return {
        "filenames": [os.path.basename(file) for file in glob.glob("static/*.json")]
    }


@app.route("/get_data")
def get_data():
    file_name = request.args.get("file")
    with open(f"static/{file_name}", "r") as json_data_file:
        return {"retrieved_data": json.load(json_data_file), "file_name": file_name}


@app.route("/get_data_gen")
def get_gen():
    """
    Generator based endpoint. Will stream data from all files in static directory.
    :return: all data from a single json file as part of stream
    """

    @stream_with_context
    def data_gen():
        file_number = 0
        #gets a list of all json files in static.
        file_names = [os.path.basename(file) for file in glob.glob("static/*.json")]
        for file in file_names:
            file_number += 1
            with open(f"static/{file}", "r") as json_data_file:
                # if we wanted to divide up the individual json files even more, put a generator in here.
                # and just pass the chunks to the dump yield below

                yield json.dumps(
                    {
                        "retrieved_data": json.load(json_data_file),
                        "file_name": file,
                        "position": f"{file_number}/{len(file_names)}",
                    }
                )
                # just so we can see the streaming functionality despite the small data set
                time.sleep(10)

    return Response(data_gen(), mimetype="application/json")


if __name__ == "__main__":
    app.run()
