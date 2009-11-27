#!/usr/bin/env python
"""
Run with:
   python crRejectStageTest.py
or
   python
   >>> import crRejectStageTest
   >>> crRejectStageTest.run()
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

from lsst.pex.harness.simpleStageTester import SimpleStageTester

try:
    type(display)
except NameError:
    display = False

#-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-

class CrRejectStageTestCase(unittest.TestCase):
    """A test case for CrRejectStage.py"""

    def setUp(self):
        filename = os.path.join(eups.productDir("afwdata"), "CFHT", "D4", "cal-53535-i-797722_1")
        bbox = afwImage.BBox(afwImage.PointI(32,32), 512, 512)
        self.exposure = afwImage.ExposureF(filename, 0,bbox)        

    def tearDown(self):
        del self.exposure        

    def testSingleExposure(self):
        
        file = pexPolicy.DefaultPolicyFile("ip_pipeline", 
                                           "crReject_policy.paf", "tests")
        policy = pexPolicy.Policy.createPolicy(file)

        stage = ipPipe.CrRejectStage(policy)
        tester = SimpleStageTester(stage)

        clipboard = pexClipboard.Clipboard()         
        clipboard.put(policy.get("inputKeys.exposure"), self.exposure)

        if display:
            ds9.mtv(self.exposure, frame=0, title="Input")
        #
        # Do the work
        #
        outWorker = tester.runWorker(clipboard)

        outPolicy = policy.get("outputKeys")
        self.assertTrue(outWorker.contains(outPolicy.get("crSubtractedExposure")))
        print "nCR = ", outWorker.get("nCR")

        if display:
            ds9.mtv(outWorker.get(outPolicy.get("crSubtractedExposure")), frame=1, title="Subtracted")

def suite():
    """Returns a suite containing all the test cases in this module."""

    utilsTests.init()

    suites = []

    if not eups.productDir("afwdata"):
        print >> sys.stderr, "afwdata is not setting up; skipping test"
    else:        
        suites += unittest.makeSuite(CrRejectStageTestCase)

    suites += unittest.makeSuite(utilsTests.MemoryTestCase)
    return unittest.TestSuite(suites)

def run(exit=False):
    """Run the tests"""
    utilsTests.run(suite(), exit)

if __name__ == "__main__":
    run(True)
