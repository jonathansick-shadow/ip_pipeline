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

from lsst.pex.harness.simpleStageTester import SimpleStageTester

try:
    type(display)
except NameError:
    display = False

#-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-

class MetadataStageTestCase(unittest.TestCase):
    """A test case for IsrLinearityStage.py"""

    def setUp(self):
        self.isrDir = eups.productDir('ip_isr')
        self.pipelineDir = eups.productDir('ip_pipeline')
        self.lutpolicypath = os.path.join(self.isrDir,"pipeline")
        self.pipepolicypath = os.path.join(self.pipelineDir,"policy")
        self.dc3MetadataFile = 'dc3MetadataPolicy.paf'
        self.cfhtDatatypeFile = 'cfhtDataTypePolicy.paf'
        self.suffix = "Keyword"
        self.imagefile = os.path.join(eups.productDir("isrdata"), "CFHT", "D4", "dc3a", "raw-704893-e000-c000-a000.fits")
        self.metadata = afwImage.readMetadata(self.imagefile)

    def tearDown(self):
        del self.isrDir
        del self.pipelineDir
        del self.lutpolicypath
        del self.pipepolicypath
        del self.dc3MetadataFile
        del self.cfhtDatatypeFile
        del self.suffix
        del self.imagefile
        del self.metadata

    def testTransformMetadata(self):
        file = pexPolicy.DefaultPolicyFile("ip_pipeline", 
                                           "TransformMetadataStageDictionary.paf",
                                           "policy")
        policy = pexPolicy.Policy.createPolicy(file)

        stage = ipPipe.TransformMetadataStage(policy)
        tester = SimpleStageTester(stage)
        
        clipboard = pexClipboard.Clipboard()
        clipboard.put(policy.get("inputKeys.inputMetadata"), self.metadata)
        clipboard.put(policy.get("inputKeys.policyPath"), self.pipepolicypath)
        clipboard.put(policy.get("inputKeys.metadataPolicyFile"),
                self.dc3MetadataFile)
        clipboard.put(policy.get("inputKeys.datatypePolicyFile"),
                self.cfhtDatatypeFile)
        clipboard.put(policy.get("inputKeys.keywordSuffix"), self.suffix)
        outWorker = tester.runWorker(clipboard)

        outPolicy = policy.get("outputKeys")
        
        self.assertTrue(outWorker.contains(outPolicy.get("transformedMetadata")))
        self.metadata = outWorker.get(outPolicy.get("transformedMetadata"))
        print self.metadata.get('gain')


def suite():
    """Returns a suite containing all the test cases in this module."""

    utilsTests.init()

    suites = []

    if not eups.productDir("isrdata"):
        print >> sys.stderr, "isrdata is not setting up; skipping test"
    else:        
        suites += unittest.makeSuite(MetadataStageTestCase)

    suites += unittest.makeSuite(utilsTests.MemoryTestCase)
    return unittest.TestSuite(suites)

def run(exit=False):
    """Run the tests"""
    utilsTests.run(suite(), exit)

if __name__ == "__main__":
    run(True)

