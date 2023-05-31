def test_func():
    compute = discovery.build('compute', 'v1')

    project = 'uber-etl-386321'
    zone = 'us-central1-a'
    instance = 'healthcare-etl-instance'

    instance_data = compute.instances().get(
        project=project, zone=zone, instance=instance).execute()
    return instance_data["metadata"]["items"]
