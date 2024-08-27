from bitfount import IntermineSource, Pod

base_url = "http://localhost:9999/camchildmine"

token = "my_intermine_user_token"

pod = Pod(
    name="intermine-datasource",
    datasource=IntermineSource(
        f"{base_url}/service",
        token=token,
        template_name="my_intermine_template",
    ),
)
pod.start()
