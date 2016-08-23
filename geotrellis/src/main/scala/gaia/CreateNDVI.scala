package gaia

import geotrellis.raster._
import geotrellis.raster.io.geotiff._
import geotrellis.raster.resample._
import geotrellis.spark._
import geotrellis.spark.io.hadoop._
import geotrellis.spark.tiling._
import geotrellis.util._

import java.io.File

import geotrellis.vector.ProjectedExtent
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

object CreateNDVI {

  /**
    * Caclulate NDVI from a multiband image (IR, Near-IR).
    *
    * @param args: an array of strings representing
    *            the following arguments: input,
    *            output, bands array as string ('1,2')
    *
    *
    */
  def main(args: Array[String]): Unit = {
    val inputImg = new File(args(0)).getAbsolutePath
    val ndviPath = new File(args(1)).getAbsolutePath
    val bands = args(2).split(',')
    val redBand = bands(0).toInt
    val irBand = bands(1).toInt
    val sparkMaster = args(3)


    val conf =
      new SparkConf()
        .setMaster(sparkMaster)
        .setAppName("Spark GeoTrellis NDVI Processor")
        .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
        .set("spark.kryo.registrator", "geotrellis.spark.io.kryo.KryoRegistrator")

    val sc = new SparkContext(conf)

    // Do the NDVI calculation
    run(sc, inputImg, ndviPath, redBand, irBand)

    // Stop the service
    sc.stop()
  }

  def run(implicit sc: SparkContext, inputPath: String, outputPath: String, redBand: Int, irBand: Int) {

    // Read the geotiff in as a single image RDD,
    // using a method implicitly added to SparkContext by
    // an implicit class available via the
    // "import geotrellis.spark.io.hadoop._ " statement.
    val inputRdd: RDD[(ProjectedExtent, MultibandTile)] =
      sc.hadoopMultibandGeoTiffRDD(inputPath)


    // Use the "TileLayerMetadata.fromRdd" call to find the zoom
    // level that the closest match to the resolution of our source image,
    // and derive information such as the full bounding box and data type.
    val (_, rasterMetaData) =
      TileLayerMetadata.fromRdd(inputRdd, FloatingLayoutScheme(512))

    // Use the Tiler to cut our tiles into tiles that are index to a floating layout scheme.
    // We'll repartition it so that there are more partitions to work with, since spark
    // likes to work with more, smaller partitions (to a point) over few and large partitions.
    val tiled: RDD[(SpatialKey, MultibandTile)] =
      inputRdd
        .tileToLayout(rasterMetaData.cellType, rasterMetaData.layout, Bilinear)
        .repartition(100)

    // Convert to a MultibandTileLayerRDD
    val multiTileRDD: (RDD[(SpatialKey, MultibandTile)] with Metadata[TileLayerMetadata[SpatialKey]]) =
      MultibandTileLayerRDD(tiled, rasterMetaData)

    // Calculate NDVI using Spark
    val raster: Raster[Tile] =
      multiTileRDD
        .withContext { rdd =>
          rdd
            .mapValues { tile =>
              tile.convert(DoubleConstantNoDataCellType).combineDouble(redBand, irBand) { (r, nir) =>
                if(isData(r) && isData(nir)) {
                  (nir - r) / (nir + r)
                } else {
                  Double.NaN
                }
              }
            }
            .map { case (key, tile) => (key.getComponent[SpatialKey], tile) }
            .reduceByKey(_.localMax(_))
        }
        .stitch

    // Save the result to GeoTIFF file
    GeoTiff(raster, multiTileRDD.metadata.crs).write(outputPath)

  }
}
