import subprocess

from gaia import formats
from gaia.core import GaiaException
from gaia.geo import RasterFileIO
from gaia.geo.gaia_process import GaiaProcess
from gaia_geotrellis import config


def parse(geoclass, inputs, output, *args):
    """ Calls the MaskRedAndInfrared scala object """

    # Jar to be called
    jar = config['gaia_geotrellis']['geotrellis_jar']

    # Memory parameter
    memory = config['gaia_geotrellis']['geotrellis_memory']

    # Command to be called by the subprocess
    command = ["java", memory, "-cp", jar, geoclass,
               ','.join([i.uri for i in inputs]), output.uri]
    print command
    output.create_output_dir(output.uri)
    process = subprocess.Popen(command, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    output, error = process.communicate()
    if error:
        raise GaiaException(u'Geotrellis error: {}'.format(error))
    return output


class GeotrellisProcess(GaiaProcess):
    """
    Calculate NDVI for a multiband image, using Geotrellis.
    """
    default_output = formats.RASTER
    required_args = ('geotrellis_class',)

    def __init__(self, **kwargs):
        super(GeotrellisProcess, self).__init__(**kwargs)
        if not self.output:
            self.output = RasterFileIO(name='result', uri=self.get_outpath())


    def compute(self):
        raise NotImplementedError


class GeotrellisNDVIProcess(GaiaProcess):
    """
    Calculate NDVI for a multiband image, using Geotrellis.
    """
    default_output = formats.RASTER
    required_inputs = (('first', formats.RASTER),)
    required_args = ('bands',)

    def __init__(self, **kwargs):
        super(GeotrellisNDVIProcess, self).__init__(**kwargs)

    def compute(self):
        result = parse('gaia.CreateNDVI', self.inputs, self.output, self.bands)
        if result:
            self.output.data = self.output.read()


class GeotrellisMaskRedIRClouds(GeotrellisProcess):
    """
    Calculate NDVI for a multiband image, using Geotrellis.
    """
    default_output = formats.RASTER
    required_inputs = (('first', formats.RASTER),)
    required_args = ('bands',)

    def __init__(self, **kwargs):
        super(GeotrellisMaskRedIRClouds, self).__init__(**kwargs)

    def compute(self):
        result = parse(
            'gaia.MaskRedAndNearInfrared', self.inputs, self.output, self.bands)
        if result:
            self.output.data = self.output.read()


PLUGIN_CLASS_EXPORTS = [
    GeotrellisNDVIProcess,
    GeotrellisMaskRedIRClouds
]
