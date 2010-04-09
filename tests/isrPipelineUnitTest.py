#!/usr/bin/env python
"""
Run with:
or
   python
"""

import sys, os, math
from math import *

import pdb
import unittest

import eups
import lsst.utils.tests as utilsTests
import lsst.pex.harness.Clipboard as pexClipboard
import lsst.pex.policy as pexPolicy
import lsst.ip.pipeline as ipPipe
import lsst.afw.image as afwImage
import lsst.afw.display.ds9 as ds9
import lsst.ip.isr as ipIsr
import lsst.afw.cameraGeom as cameraGeom
import lsst.afw.cameraGeom.utils as cameraGeomUtils
import lsst.meas.algorithms.defects as measDefects

from lsst.pex.harness.simpleStageTester import SimpleStageTester

try:
    type(display)
except NameError:
    display = False

#-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-

class IsrStageTestCase(unittest.TestCase):
    """A test case for IsrLinearityStage.py"""

    def setUp(self):
        afwDataDir = eups.productDir("afwdata")
        self.obsDir = eups.productDir('obs_lsstSim')
        self.isrDir = eups.productDir('ip_isr')
        self.pipelineDir = eups.productDir('ip_pipeline')
        self.lutpolicypath = os.path.join(self.isrDir,"pipeline")
        self.pipepolicypath = os.path.join(self.pipelineDir,"policy")
        self.dc3MetadataFile = 'dc3MetadataPolicy.paf'
        self.simDatatypeFile = 'simDataTypePolicy.paf'
        self.suffix = "Keyword"
        policyFile = pexPolicy.DefaultPolicyFile("afw",
                "CameraGeomDictionary.paf", "policy")
        defPolicy = pexPolicy.Policy.createPolicy(policyFile,
                policyFile.getRepositoryPath(), True)

        geomPolicy =\
        pexPolicy.Policy.createPolicy(os.path.join(self.obsDir,"description","Full_STA_geom.paf"),
                    True)
        geomPolicy.mergeDefaults(defPolicy.getDictionary())
        self.cameraPolicy = geomPolicy
        self.imagefile = os.path.join(afwDataDir, "ImSim/",
                "imsim_85751839_R23_S11_C00_E000.fits.gz")
        self.biasfile = os.path.join(afwDataDir, "ImSim/bias",
                "imsim_0_R23_S11_C00_E000")
        self.darkfile = os.path.join(afwDataDir, "ImSim/dark",
                "imsim_1_R23_S11_C00_E000")
        self.flatfile = os.path.join(afwDataDir, "ImSim/flat_r",
                "imsim_2_R23_S11_C00_E000")
        # Note that we have no fringe frame for Sim data at the moment.  I put
        # in the flat as proxy for the fringe frame.
        inputImage = afwImage.ImageF(self.imagefile)
        self.metadata = afwImage.readMetadata(self.imagefile)

        ipIsr.transformMetadata(self.metadata,
                pexPolicy.Policy.createPolicy(os.path.join(self.pipepolicypath,self.simDatatypeFile)),
                pexPolicy.Policy.createPolicy(os.path.join(self.pipepolicypath,self.dc3MetadataFile)),
                self.suffix)
        self.exposureBbox = afwImage.BBox(afwImage.PointI(0,0), inputImage.getWidth(),
                    inputImage.getHeight())
        self.exposure = ipIsr.exposureFromInputData(inputImage, self.metadata,
                self.exposureBbox)
        self.id = cameraGeom.Id(-1, "R:2,3 S:1,1 C:0,0", 0, 0)


    def tearDown(self):
        for key in self.__dict__.keys():
          del self.__dict__[key]

    def testPipe(self):
        """Pipeline test case."""

        clipboard = pexClipboard.Clipboard()         
        clipboard.put("ampId", self.id)
        clipboard.put("cameraPolicy", self.cameraPolicy )
        clipboard.put("Exposure", self.exposure)
        clipboard.put("fwhm", 5.)
        file = pexPolicy.DefaultPolicyFile("ip_pipeline", 
                                           "MakeCameraGeomStageDictionary.paf",
                                           "policy")
        p1 = pexPolicy.Policy.createPolicy(file)
        s1 = ipPipe.MakeCameraStage(p1)
        t1 = SimpleStageTester(s1)
        file = pexPolicy.DefaultPolicyFile("ip_pipeline", 
                                           "IsrSaturationStageDictionary.paf",
                                           "policy")
        p2 = pexPolicy.Policy.createPolicy(file)
        s2 = ipPipe.IsrSaturationStage(p2)
        t2 = SimpleStageTester(s2)
        file = pexPolicy.DefaultPolicyFile("ip_pipeline", 
                                           "IsrOverscanStageDictionary.paf",
                                           "policy")
        p3 = pexPolicy.Policy.createPolicy(file)
        clipboard.put(p3.get("inputKeys.overscanfittype"), "MEDIAN")
        s3 = ipPipe.IsrOverscanStage(p3)
        t3 = SimpleStageTester(s3)
        file = pexPolicy.DefaultPolicyFile("ip_pipeline", 
                                           "IsrBiasStageDictionary.paf",
                                           "policy")
        p4 = pexPolicy.Policy.createPolicy(file)
        s4 = ipPipe.IsrBiasStage(p4)
        t4 = SimpleStageTester(s4)
        file = pexPolicy.DefaultPolicyFile("ip_pipeline", 
                                           "IsrDarkStageDictionary.paf",
                                           "policy")
        p5 = pexPolicy.Policy.createPolicy(file)
        s5 = ipPipe.IsrDarkStage(p5)
        t5 = SimpleStageTester(s5)
        bias = afwImage.ExposureF(self.biasfile)
        clipboard.put(p4.get("inputKeys.biasexposure"), bias)

        dark = afwImage.ExposureF(self.darkfile)
        clipboard.put(p5.get("inputKeys.darkexposure"), dark)
        clipboard.put(p5.get("inputKeys.exposurescale"),
                self.exposure.getMetadata().getDouble('expTime'))
        clipboard.put(p5.get("inputKeys.darkscale"),
                dark.getMetadata().getDouble('EXPTIME'))

        file = pexPolicy.DefaultPolicyFile("ip_pipeline", 
                                           "IsrFlatStageDictionary.paf",
                                           "policy")
        p6 = pexPolicy.Policy.createPolicy(file)
        s6 = ipPipe.IsrFlatStage(p6)
        t6 = SimpleStageTester(s6)
        flat = afwImage.ExposureF(self.flatfile)
        clipboard.put(p6.get("inputKeys.flatexposure"), flat)
        clipboard.put(p6.get("inputKeys.flatscalingtype"), 'MEAN')
        o1 = t1.runWorker(clipboard)
        o2 = t2.runWorker(o1)
        o2.put(p3.get("inputKeys.exposure"),
            o2.get(p2.get("outputKeys.saturationCorrectedExposure")))
        o3 = t3.runWorker(o2)
        o3.put(p4.get("inputKeys.exposure"),
            o3.get(p3.get("outputKeys.overscanCorrectedExposure")))
        o4 = t4.runWorker(o3)
        o4.put(p5.get("inputKeys.exposure"),
            o4.get(p4.get("outputKeys.biasSubtractedExposure")))
        o5 = t5.runWorker(o4)
        o5.put(p6.get("inputKeys.exposure"),
            o5.get(p5.get("outputKeys.darkSubtractedExposure")))
        o6 = t6.runWorker(o5)
        self.exposure = o6.get(p6.get("outputKeys.flatCorrectedExposure"))
        if display:
            ds9.mtv(self.exposure, frame=0, title="Output")


def suite():
    """Returns a suite containing all the test cases in this module."""

    utilsTests.init()

    suites = []

    if not eups.productDir("isrdata"):
        print >> sys.stderr, "isrdata is not setting up; skipping test"
    else:        
        suites += unittest.makeSuite(IsrStageTestCase)

    suites += unittest.makeSuite(utilsTests.MemoryTestCase)
    return unittest.TestSuite(suites)

def run(exit=False):
    """Run the tests"""
    utilsTests.run(suite(), exit)

if __name__ == "__main__":
    run(True)

