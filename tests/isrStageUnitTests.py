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
        isrDataDir = eups.productDir("isrdata")
        self.isrDir = eups.productDir('ip_isr')
        self.pipelineDir = eups.productDir('ip_pipeline')
        self.lutpolicypath = os.path.join(self.isrDir,"pipeline")
        self.pipepolicypath = os.path.join(self.pipelineDir,"policy")
        self.dc3MetadataFile = 'dc3MetadataPolicy.paf'
        self.cfhtDatatypeFile = 'cfhtDataTypePolicy.paf'
        self.suffix = "Keyword"
        self.ampBBox = afwImage.BBox(afwImage.PointI(0,0), 1024, 1153)
        self.imagefile = os.path.join(isrDataDir, "CFHT/D4/dc3a", "raw-704893-e000-c000-a000.fits")
        self.biasfile = os.path.join(isrDataDir, "CFHT/D4/dc3a", "bias-0-c000-a000")
        self.darkfile = os.path.join(isrDataDir, "CFHT/D4/dc3a", "dark-300-c000-a000")
        self.flatfile = os.path.join(isrDataDir, "CFHT/D4/dc3a", "flat-i-c000-a000")
        # Note that we have no fringe frame for Sim data at the moment.  I put
        # in the flat as proxy for the fringe frame.
        self.fringefile = os.path.join(isrDataDir, "CFHT/D4/dc3a", "flat-i-c000-a000")
        self.defectfile = os.path.join(isrDataDir, "CFHT/D4/dc3a", "defect-c000-a000.paf")
        self.flatscalingtype = 'MEAN'
        self.lutfilename = "linearizationLookupTable.paf"
        self.saturation = 1000.
        self.fwhm = 5.
        self.ofittype = "MEAN"
        inputImage = afwImage.ImageF(self.imagefile)
        self.metadata = afwImage.readMetadata(self.imagefile)
        ipIsr.transformMetadata(self.metadata,
                pexPolicy.Policy.createPolicy(os.path.join(self.pipepolicypath,self.cfhtDatatypeFile)),
                pexPolicy.Policy.createPolicy(os.path.join(self.pipepolicypath,self.dc3MetadataFile)),
                self.suffix)
        self.oBbox = ipIsr.BBoxFromDatasec(self.metadata.get('overscan'))
        self.tBbox = ipIsr.BBoxFromDatasec(self.metadata.get('trimsec'))
        self.exposureBbox = afwImage.BBox(afwImage.PointI(0,0), inputImage.getWidth(),
                    inputImage.getHeight())
        self.exposure = ipIsr.exposureFromInputData(inputImage, self.metadata,
                self.exposureBbox)


    def tearDown(self):
        del self.isrDir
        del self.pipelineDir
        del self.lutpolicypath
        del self.pipepolicypath
        del self.dc3MetadataFile
        del self.cfhtDatatypeFile
        del self.suffix
        del self.imagefile
        del self.lutfilename
        del self.saturation
        del self.fwhm
        del self.oBbox
        del self.tBbox
        del self.ofittype
        del self.metadata
        del self.exposure
        del self.exposureBbox


    def testLinearity(self):
        
        file = pexPolicy.DefaultPolicyFile("ip_pipeline", 
                                           "IsrLinearityStageDictionary.paf",
                                           "policy")
        policy = pexPolicy.Policy.createPolicy(file)

        stage = ipPipe.IsrLinearityStage(policy)
        tester = SimpleStageTester(stage)

        clipboard = pexClipboard.Clipboard()         
        clipboard.put(policy.get("inputKeys.exposure"), self.exposure)
        clipboard.put(policy.get("inputKeys.lutPolicyPath"), self.lutpolicypath)
        clipboard.put(policy.get("inputKeys.lutPolicyFile"), self.lutfilename )

        if display:
            ds9.mtv(self.exposure, frame=0, title="Input")
        #
        # Do the work
        #
        outWorker = tester.runWorker(clipboard)

        outPolicy = policy.get("outputKeys")
        self.assertTrue(outWorker.contains(outPolicy.get("linearityCorrectedExposure")))

        if display:
            ds9.mtv(outWorker.get(outPolicy.get("linearityCorrectedExposure")),
                    frame=1, title="linearized")

    def testSaturation(self):
        
        file = pexPolicy.DefaultPolicyFile("ip_pipeline", 
                                           "IsrSaturationStageDictionary.paf",
                                           "policy")
        policy = pexPolicy.Policy.createPolicy(file)

        stage = ipPipe.IsrSaturationStage(policy)
        tester = SimpleStageTester(stage)

        clipboard = pexClipboard.Clipboard()         
        clipboard.put(policy.get("inputKeys.exposure"), self.exposure)
        clipboard.put(policy.get("inputKeys.saturation"), self.saturation)
        clipboard.put(policy.get("inputKeys.fwhm"), self.fwhm)

        if display:
            ds9.mtv(self.exposure, frame=0, title="Input")
        #
        # Do the work
        #
        outWorker = tester.runWorker(clipboard)

        outPolicy = policy.get("outputKeys")
        self.assertTrue(outWorker.contains(outPolicy.get("saturationCorrectedExposure")))

        if display:
            ds9.mtv(outWorker.get(outPolicy.get("saturationCorrectedExposure")),
                    frame=1, title="Sat Corrected Interpolated")

    def testOverscan(self):
        
        file = pexPolicy.DefaultPolicyFile("ip_pipeline", 
                                           "IsrOverscanStageDictionary.paf",
                                           "policy")
        policy = pexPolicy.Policy.createPolicy(file)

        stage = ipPipe.IsrOverscanStage(policy)
        tester = SimpleStageTester(stage)

        clipboard = pexClipboard.Clipboard()         
        clipboard.put(policy.get("inputKeys.exposure"), self.exposure)
        clipboard.put(policy.get("inputKeys.overscansec"), self.oBbox)
        clipboard.put(policy.get("inputKeys.overscanfittype"), self.ofittype)

        if display:
            ds9.mtv(self.exposure, frame=0, title="Input")
        #
        # Do the work
        #
        outWorker = tester.runWorker(clipboard)

        outPolicy = policy.get("outputKeys")
        self.assertTrue(outWorker.contains(outPolicy.get("overscanCorrectedExposure")))

        if display:
            ds9.mtv(outWorker.get(outPolicy.get("overscanCorrectedExposure")),
                    frame=1, title="Overscan Corrected")

    def testBiasCorrect(self):
        
        file = pexPolicy.DefaultPolicyFile("ip_pipeline", 
                                           "IsrBiasStageDictionary.paf",
                                           "policy")
        policy = pexPolicy.Policy.createPolicy(file)

        stage = ipPipe.IsrBiasStage(policy)
        tester = SimpleStageTester(stage)

        bias = afwImage.ExposureF(self.biasfile)
        newExposure = ipIsr.trimNew(self.exposure, self.ampBBox)

        clipboard = pexClipboard.Clipboard()         
        clipboard.put(policy.get("inputKeys.exposure"), newExposure)
        clipboard.put(policy.get("inputKeys.biasexposure"), bias)

        if display:
            ds9.mtv(self.exposure, frame=0, title="Input")
        #
        # Do the work
        #
        outWorker = tester.runWorker(clipboard)

        outPolicy = policy.get("outputKeys")
        self.assertTrue(outWorker.contains(outPolicy.get("biasSubtractedExposure")))

        if display:
            ds9.mtv(outWorker.get(outPolicy.get("biasSubtractedExposure")),
                    frame=1, title="Bias Subtracted")

    def testDarkCorrect(self):
        
        file = pexPolicy.DefaultPolicyFile("ip_pipeline", 
                                           "IsrDarkStageDictionary.paf",
                                           "policy")
        policy = pexPolicy.Policy.createPolicy(file)

        stage = ipPipe.IsrDarkStage(policy)
        tester = SimpleStageTester(stage)

        dark = afwImage.ExposureF(self.darkfile)
        darkmetadata = dark.getMetadata()
        ipIsr.transformMetadata(darkmetadata,
                pexPolicy.Policy.createPolicy(os.path.join(self.pipepolicypath,self.cfhtDatatypeFile)),
                pexPolicy.Policy.createPolicy(os.path.join(self.pipepolicypath,self.dc3MetadataFile)),
                self.suffix)

        newExposure = ipIsr.trimNew(self.exposure, self.ampBBox)

        clipboard = pexClipboard.Clipboard()         
        clipboard.put(policy.get("inputKeys.exposure"), newExposure)
        clipboard.put(policy.get("inputKeys.darkexposure"), dark)
        clipboard.put(policy.get("inputKeys.darkscale"), darkmetadata.get('expTime'))
        clipboard.put(policy.get("inputKeys.exposurescale"), self.metadata.get('expTime'))


        if display:
            ds9.mtv(self.exposure, frame=0, title="Input")
        #
        # Do the work
        #
        outWorker = tester.runWorker(clipboard)

        outPolicy = policy.get("outputKeys")
        self.assertTrue(outWorker.contains(outPolicy.get("darkSubtractedExposure")))

        if display:
            ds9.mtv(outWorker.get(outPolicy.get("darkSubtractedExposure")),
                    frame=1, title="Dark Subtracted")

    def testFlatCorrect(self):
        
        file = pexPolicy.DefaultPolicyFile("ip_pipeline", 
                                           "IsrFlatStageDictionary.paf",
                                           "policy")
        policy = pexPolicy.Policy.createPolicy(file)

        stage = ipPipe.IsrFlatStage(policy)
        tester = SimpleStageTester(stage)

        flat = afwImage.ExposureF(self.flatfile)

        newExposure = ipIsr.trimNew(self.exposure, self.ampBBox)

        clipboard = pexClipboard.Clipboard()         
        clipboard.put(policy.get("inputKeys.exposure"), newExposure)
        clipboard.put(policy.get("inputKeys.flatexposure"), flat)
        clipboard.put(policy.get("inputKeys.flatscalingtype"),
                self.flatscalingtype)


        if display:
            ds9.mtv(self.exposure, frame=0, title="Input")
        #
        # Do the work
        #
        outWorker = tester.runWorker(clipboard)

        outPolicy = policy.get("outputKeys")
        self.assertTrue(outWorker.contains(outPolicy.get("flatCorrectedExposure")))

        if display:
            ds9.mtv(outWorker.get(outPolicy.get("flatCorrectedExposure")),
                    frame=1, title="Flat Corrected")

    def testFringeCorrect(self):
        
        file = pexPolicy.DefaultPolicyFile("ip_pipeline", 
                                           "IsrFringeStageDictionary.paf",
                                           "policy")
        policy = pexPolicy.Policy.createPolicy(file)

        stage = ipPipe.IsrFringeStage(policy)
        tester = SimpleStageTester(stage)

        fringe = afwImage.ExposureF(self.fringefile)

        newExposure = ipIsr.trimNew(self.exposure, self.ampBBox)

        clipboard = pexClipboard.Clipboard()         
        clipboard.put(policy.get("inputKeys.exposure"), newExposure)
        clipboard.put(policy.get("inputKeys.fringeexposure"), fringe)


        if display:
            ds9.mtv(self.exposure, frame=0, title="Input")
        #
        # Do the work
        #
        outWorker = tester.runWorker(clipboard)

        outPolicy = policy.get("outputKeys")
        self.assertTrue(outWorker.contains(outPolicy.get("fringeCorrectedExposure")))

        if display:
            ds9.mtv(outWorker.get(outPolicy.get("fringeCorrectedExposure")),
                    frame=1, title="Fringe Corrected:!!! Not implemented yet.")

    def testDefectCorrect(self):
        
        file = pexPolicy.DefaultPolicyFile("ip_pipeline", 
                                           "IsrDefectStageDictionary.paf",
                                           "policy")
        policy = pexPolicy.Policy.createPolicy(file)

        stage = ipPipe.IsrDefectStage(policy)
        tester = SimpleStageTester(stage)

        defectList = measDefects.policyToBadRegionList(self.defectfile)

        newExposure = ipIsr.trimNew(self.exposure, self.ampBBox)

        clipboard = pexClipboard.Clipboard()         
        clipboard.put(policy.get("inputKeys.exposure"), newExposure)
        clipboard.put(policy.get("inputKeys.defectList"), defectList)
        clipboard.put(policy.get("inputKeys.fwhm"), self.fwhm)


        if display:
            ds9.mtv(self.exposure, frame=0, title="Input")
        #
        # Do the work
        #
        outWorker = tester.runWorker(clipboard)

        outPolicy = policy.get("outputKeys")
        self.assertTrue(outWorker.contains(outPolicy.get("defectCorrectedExposure")))

        if display:
            ds9.mtv(outWorker.get(outPolicy.get("defectCorrectedExposure")),
                    frame=1, title="Defect Corrected")

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

