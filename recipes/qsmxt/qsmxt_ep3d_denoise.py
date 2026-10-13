"""OpenRecon config for 3D EPI dual polarity QSM with NLM denoising."""

import qsmxt_ep3d


def process(connection, config, metadata):
    qsmxt_ep3d.process(connection, config, metadata, denoise=True)
