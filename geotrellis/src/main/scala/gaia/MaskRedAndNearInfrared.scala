package gaia

import geotrellis.raster._
import geotrellis.raster.io.geotiff._
import geotrellis.raster.render._
import com.typesafe.config.ConfigFactory
import java.io.File
import geotrellis.proj4.CRS
import geotrellis.vector.Extent

object MaskRedAndNearInfrared {

  def main(args: Array[String]): Unit = {

    val inputs = args(0).split(',')
    val outputPath = new File(args(1)).getAbsolutePath
    val bands = if (args.length == 3) args(2).split(',') else new Array[String](0)

    def getBands(files: Array[String]): (Tile, Tile, Tile, Extent, CRS) =
      if (inputs.length == 3) {
        // Read in the infrared band
        val rGeoTiff = SinglebandGeoTiff(new File(inputs(0).trim()).getAbsolutePath)

        // Read in the near infrared band
        val nirGeoTiff = SinglebandGeoTiff(new File(inputs(1).trim()).getAbsolutePath)

        // Read in the QA band
        val qaGeoTiff = SinglebandGeoTiff(new File(inputs(2).trim()).getAbsolutePath)

        // GeoTiffs have more information we need; just grab the Tile out of them.
        (rGeoTiff.tile, nirGeoTiff.tile, qaGeoTiff.tile, rGeoTiff.extent, rGeoTiff.crs)

      } else if (inputs.length == 1 & bands.length == 3) {
        val geoGTiff = MultibandGeoTiff(new File(inputs(0).trim()).getAbsolutePath)
          // GeoTiffs have more information we need; just grab the Tile out of them.
          (
            geoGTiff.band(bands(0).trim().toInt),
            geoGTiff.band(bands(1).trim().toInt),
            geoGTiff.band(bands(2).trim().toInt),
            geoGTiff.extent, geoGTiff.crs
          )
      } else {
        throw new IllegalArgumentException("Must have 3 inputs and 3 bands, or 1 input")
      }

    val (rTile, nirTile, qaTile, extent, crs) = getBands(inputs)

    // This function will set anything that is potentially a cloud to NODATA
    def maskClouds(tile: Tile): Tile =
      tile.combine(qaTile) { (v: Int, qa: Int) =>
        val isCloud = qa & 0x8000
        val isCirrus = qa & 0x2000
        if(isCloud > 0 || isCirrus > 0) { NODATA }
        else { v }
      }

    // Mask our red and near infrared bands using the qa band
    val rMasked = maskClouds(rTile)
    val nirMasked = maskClouds(nirTile)

    // Create a multiband tile with our two masked red and infrared bands.
    val mb = ArrayMultibandTile(rMasked, nirMasked).convert(IntConstantNoDataCellType)

    // Create a multiband geotiff from our tile, using the same extent and CRS as the original geotiffs.
    MultibandGeoTiff(mb, extent, crs).write(outputPath)
  }
}
