import subprocess

from gaia import formats
from gaia.core import GaiaException
from gaia.geo import RasterFileIO
from gaia.geo.gaia_process import GaiaProcess
from gaia_geotrellis import config


def pipe2geotrellis(geoclass, inputs, output, args):
    """ Executes a Geotrellis class and returns result """

    # Jar to be called
    jar = config['gaia_geotrellis']['geotrellis_jar']

    # Memory parameter
    memory = config['gaia_geotrellis']['geotrellis_memory']

    #Spark Master
    spark = config['gaia_geotrellis']['spark_master']

    # Command to be called by the subprocess
    command = ["java", memory, "-cp", jar, geoclass,
               ','.join([i.uri for i in inputs]), output.uri] + args
    command.append(spark)
    output.create_output_dir(output.uri)
    process = subprocess.Popen(command, stdout=subprocess.PIPE,
                               stderr=subprocess.PIPE)
    result, error = process.communicate()
    if process.returncode != 0:
        raise GaiaException(u'Geotrellis error: {}'.format(error))
    return result


class GeotrellisProcess(GaiaProcess):
    """
    Base class for running Geotrellis processes
    """
    default_output = formats.RASTER
    required_args = ('gt_class',)
    io = ('inputs', 'output', 'gt_class')

    def __init__(self, **kwargs):
        self.args = [kwargs[key] for key in kwargs.keys() if key not in self.io]
        super(GeotrellisProcess, self).__init__(**kwargs)
        if not self.output:
            self.output = RasterFileIO(name='result', uri=self.get_outpath())

    def compute(self):
        result = pipe2geotrellis(self.gt_class, self.inputs,
                                 self.output, self.args)
        if result:
            self.output.data = self.output.read()


class GeotrellisNDVIProcess(GeotrellisProcess):
    """
    Calculate NDVI for a multiband image, using Geotrellis.
    'bands' should be supplied as a keyword argument, with
    a string representation of the IR and near-IR bands,
    for example '2,3'
    """
    default_output = formats.RASTER
    required_inputs = (('first', formats.RASTER),)
    required_args = ('bands',)
    gt_class = 'gaia.CreateNDVI'

    def __init__(self, **kwargs):
        super(GeotrellisNDVIProcess, self).__init__(**kwargs)
        if len(self.bands.split(',')) != 2:
            raise GaiaException("You must supply band numbers for the "
                                "IR and Near-IR bands")
        elif len(self.inputs) != 1:
            raise GaiaException("You must have 1 multiband input raster")


class GeotrellisCloudMaskProcess(GeotrellisProcess):
    """
    Mask clouds from a multiband image or series of images, using Geotrellis.
    There should be either 3 inputs (IR, Near-IR, QA single-band geotiffs) or
    one multiband image and a bands argument supplied as a keyword argument,
    with a string representation of the IR, near-IR, and QA bands,
    for example '2,3, 4'
    """
    default_output = formats.RASTER
    required_inputs = (('first', formats.RASTER),)
    required_args = ('bands',)
    gt_class = 'gaia.MaskRedAndNearInfrared'

    def __init__(self, **kwargs):
        super(GeotrellisCloudMaskProcess, self).__init__(**kwargs)
        if len(self.inputs) == 1 and len(self.bands.split(',')) != 3:
            raise GaiaException("You must supply band numbers for the "
                                "IR, Near-IR, and QA bands")
        elif len(self.inputs) != 3:
            raise GaiaException("You must have 1 multiband or 3 "
                                "singleband input rasters")

PLUGIN_CLASS_EXPORTS = [
    GeotrellisNDVIProcess,
    GeotrellisCloudMaskProcess
]
