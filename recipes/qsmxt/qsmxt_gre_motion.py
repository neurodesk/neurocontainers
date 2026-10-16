import qsmxt


def process(connection, config, metadata):
    parameters = dict(qsmxt._config_parameters(config), motioncorrection=True)
    qsmxt.process(connection, {"parameters": parameters}, metadata)
