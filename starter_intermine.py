import sys
print(sys.path)

from bitfount import IntermineSource, Pod

# import requests

base_url = "http://localhost:9999/camchildmine"

# response = requests.get(f"{base_url}/query/service/user/session")
# token = response.json()["token"]


token = "V1v9c485Pfk8m8n0Q4q4"

pod = Pod(
    name="intermine-datasource",
    datasource=IntermineSource(
        f"{base_url}/service",
        token=token,
        template_name="problem_patient",
    ),
)
pod.start()
