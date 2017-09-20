package org.examples.rnn

import java.io.File
import java.nio.charset.Charset
import java.nio.file.Files

import org.apache.commons.logging.LogFactory
import org.deeplearning4j.datasets.iterator.{AbstractDataSetIterator, DoublesDataSetIterator}
import org.deeplearning4j.nn.api.{Model, OptimizationAlgorithm}
import org.deeplearning4j.nn.conf.layers.{GravesLSTM, RnnOutputLayer}
import org.deeplearning4j.nn.conf.{BackpropType, NeuralNetConfiguration, Updater}
import org.deeplearning4j.nn.graph.ComputationGraph
import org.deeplearning4j.nn.multilayer.MultiLayerNetwork
import org.deeplearning4j.nn.weights.WeightInit
import org.deeplearning4j.optimize.api.IterationListener
import org.deeplearning4j.optimize.listeners.ScoreIterationListener
import org.deeplearning4j.parallelism.ParallelWrapper
import org.nd4j.linalg.activations.Activation
import org.nd4j.linalg.factory.Nd4j
import org.nd4j.linalg.lossfunctions.LossFunctions.LossFunction

import scala.collection.JavaConverters._
import scala.util.Random

class GravesLSTMIntModelling

object GravesLSTMIntModelling {
  val log = LogFactory.getLog(classOf[GravesLSTMIntModelling])
  //Number of units in each GravesLSTM layer
  val lstmLayerSize = 30
  //Total number of training epochs
  val numEpochs = 1
  //How frequently to generate samples from the network? 1000 characters / 50 tbptt length: 20 parameter updates per minibatch
  val generateSamplesEveryNMinibatches = 10
  //Number of samples to generate after each training epoch
  val nSamplesToGenerate = 4
  //Length of each sample to generate
  val nCharactersToSample = 10
  //Optional character initialization; a random character is used if null
  val generationInitialization = 1000000

  def main(args: Array[String]): Unit = {
    val testDatas = Files.readAllLines(new File("/home/wpy/tmp/data.txt").toPath, Charset.defaultCharset()).iterator().asScala.zipWithIndex.map { kv => new org.nd4j.linalg.primitives.Pair(Array(kv._2.toDouble), Array(kv._1.toDouble)) }
    val iter = new DoublesDataSetIterator(testDatas.toIterable.asJava, 1)
    val rng = new Random(12345)
    val nOut = 1

    val conf = new NeuralNetConfiguration.Builder()
      .optimizationAlgo(OptimizationAlgorithm.STOCHASTIC_GRADIENT_DESCENT).iterations(1)
      .learningRate(0.1)
      .seed(12345)
      .regularization(true)
      .l2(0.001)
      .weightInit(WeightInit.XAVIER)
      .graphBuilder()
      .addInputs("input")
      .addLayer("first", new GravesLSTM.Builder().nIn(iter.inputColumns()).nOut(lstmLayerSize)
        .updater(Updater.RMSPROP).activation(Activation.TANH).build(), "input")
      .addLayer("second", new GravesLSTM.Builder().nIn(lstmLayerSize).nOut(lstmLayerSize)
        .updater(Updater.RMSPROP).activation(Activation.TANH).build(), "first")
      .addLayer("outputLayer", new RnnOutputLayer.Builder(LossFunction.MCXENT)
        .activation(Activation.SOFTMAX).updater(Updater.RMSPROP) //MCXENT + softmax for classification
        .nIn(2 * lstmLayerSize).nOut(nOut).build(), "first", "second")
      .setOutputs("outputLayer")
      .backpropType(BackpropType.Standard)
      .pretrain(false).backprop(true)
      .build()

    val net = new ComputationGraph(conf)
    net.init()
    net.setListeners(new ScoreIterationListener(1) /*new IterationListener() {
      override def invoke() = {}

      override def iterationDone(model: Model, iteration: Int) = {
        log.info("---------------------------------------------------------------")
        log.info(s"network given initialization: $generationInitialization")
        val samples = sampleFromNetwork(generationInitialization, model, iter, nCharactersToSample, nSamplesToGenerate, rng)
        log.info(samples.zipWithIndex.mkString("\n"))
      }

      override def invoked() = true
    }*/)

    val totalNumParams = net.getLayers.foldLeft(0)(_ + _.numParams())
    log.info(s"total number of network parameters: $totalNumParams")

    for (_ <- 0 until numEpochs) {
      iter.asScala.zipWithIndex.foreach { ds =>
        net.fit(ds._1)
        if ((ds._2 + 1) % 10 == 0) {
          log.info("-----------------------------------------")
          val samples = sampleFromNetwork(generationInitialization, net, iter, nCharactersToSample, nSamplesToGenerate, rng)
          println(samples.zipWithIndex.mkString("\n"))
        }
      }
    }
    System.out.println("\n\nExample complete")

  }

  def sampleFromNetwork(initialization: Int, net: ComputationGraph, iter: DoublesDataSetIterator, intsToSample: Int, numSamples: Int, rng: Random) = {
    val initializationInput = Nd4j.zeros(numSamples, iter.inputColumns)
    for (i <- 0 until numSamples) initializationInput.putScalar(i, 1.0f)
    val sb = new Array[StringBuilder](numSamples)
    for (i <- sb.indices) sb(i) = new StringBuilder(initialization)

    net.rnnClearPreviousState()
    val o = net.rnnTimeStep(initializationInput)(0)
    val output = o.tensorAlongDimension(o.size(1) - 1, 1, 0)
    for (_ <- 0 until intsToSample) {
      val nextInput = Nd4j.zeros(numSamples, iter.inputColumns())
      for (s <- 0 until numSamples) {
        val outputProbDistribution = (for (j <- 0 until iter.totalOutcomes()) yield
          output.getDouble(j)).toArray
        val sampledIntIdx = sampleFromDistribution(outputProbDistribution, rng)
        nextInput.putScalar(Array(s, sampledIntIdx), 1.0f)
        sb(s).append(sampledIntIdx)
      }
    }
    for (i <- 0 until numSamples) yield sb(i).toString()
  }

  def sampleFromDistribution(outputProbDistribution: Array[Double], rng: Random): Int = {
    val d = rng.nextDouble()
    var sum = 0.0D
    for (i <- outputProbDistribution.indices) {
      sum += outputProbDistribution(i)
      if (d <= sum) return i
    }
    throw new IllegalArgumentException("Distribution is invalid? i=" + d + ", sum=" + sum)
  }
}
