#!/usr/bin/env python

# 
# LSST Data Management System
# Copyright 2008, 2009, 2010 LSST Corporation.
# 
# This product includes software developed by the
# LSST Project (http://www.lsst.org/).
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
# 
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
# 
# You should have received a copy of the LSST License Statement and 
# the GNU General Public License along with this program.  If not, 
# see <http://www.lsstcorp.org/LegalNotices/>.
#

"""
Run with:
   python diffImStageTest.py
or
   python
   >>> import diffImStageTest
   >>> diffImStageTest.run()
"""

import sys, os, math
from math import *

import pdb
import unittest

import eups
import lsst.utils.tests as utilsTests
import lsst.pex.harness.Clipboard as pexClipboard
import lsst.pex.policy as pexPolicy
import lsst.pex.logging as logging
import lsst.ip.pipeline as ipPipe
import lsst.afw.image as afwImage
import lsst.afw.math as afwMath
import lsst.afw.display.ds9 as ds9

from lsst.ip.diffim import diffimTools

from lsst.pex.harness.simpleStageTester import SimpleStageTester

try:
    type(display)
except NameError:
    display = False
    
Verbosity = 5
logging.Trace_setVerbosity('lsst.ip.diffim', Verbosity)

#-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-

class ValidationTestCase(unittest.TestCase):
    """A test case for policy validation"""
    def getTestDictionary(self, filename=None): 
        directory = os.path.join(eups.productDir("ip_pipeline"), "policy") 
        return os.path.join(directory, filename) if filename else directory
    
    def testValidation(self):
        policyFile = pexPolicy.DefaultPolicyFile("ip_pipeline", 
                                             "DiffImStageDictionary.paf", "policy")
        defPolicy = pexPolicy.Policy.createPolicy(policyFile, policyFile.getRepositoryPath(), True)

        # only 3,4,5 allowed
        try:
            policy = pexPolicy.Policy()
            policy.set("diffImPolicy.warpingKernelName", "lanczos2")
            policy.mergeDefaults(defPolicy.getDictionary())
        except:
            pass
        else:
            self.fail()
        try:
            policy = pexPolicy.Policy()
            policy.set("diffImPolicy.warpingKernelName", "lanczos6")
            policy.mergeDefaults(defPolicy.getDictionary())
        except:
            pass
        else:
            self.fail()

        try:
            policy = pexPolicy.Policy()
            policy.set("diffImPolicy.detThresholdType", "countttts")
            policy.mergeDefaults(defPolicy.getDictionary())
        except:
            pass
        else:
            self.fail()

        try:
            policy = pexPolicy.Policy()
            policy.set("diffImPolicy.kernelBasisSet", "bogus")
            policy.mergeDefaults(defPolicy.getDictionary())
        except:
            pass
        else:
            self.fail()

        # only 0,1,2 allowed
        try:
            policy = pexPolicy.Policy()
            policy.set("diffImPolicy.regularizationOrder", -1)
            policy.mergeDefaults(defPolicy.getDictionary())
        except:
            pass
        else:
            self.fail()
        try:
            policy = pexPolicy.Policy()
            policy.set("diffImPolicy.regularizationOrder", 3)
            policy.mergeDefaults(defPolicy.getDictionary())
        except:
            pass
        else:
            self.fail()

        # only 0,1,2 allowed
        try:
            policy = pexPolicy.Policy()
            policy.set("diffImPolicy.regularizationBoundary", -1)
            policy.mergeDefaults(defPolicy.getDictionary())
        except:
            pass
        else:
            self.fail()
        try:
            policy = pexPolicy.Policy()
            policy.set("diffImPolicy.regularizationBoundary", 3)
            policy.mergeDefaults(defPolicy.getDictionary())
        except:
            pass
        else:
            self.fail()

        # only 0,1 allowed
        try:
            policy = pexPolicy.Policy()
            policy.set("diffImPolicy.regularizationDifference", -1)
            policy.mergeDefaults(defPolicy.getDictionary())
        except:
            pass
        else:
            self.fail()
        try:
            policy = pexPolicy.Policy()
            policy.set("diffImPolicy.regularizationDifference", 2)
            policy.mergeDefaults(defPolicy.getDictionary())
        except:
            pass
        else:
            self.fail()

        try:
            policy = pexPolicy.Policy()
            policy.set("diffImPolicy.backgroundPolicy.algorithm", "MAKE_IT_UP")
            policy.mergeDefaults(defPolicy.getDictionary())
        except:
            pass
        else:
            self.fail()
        try:
            policy = pexPolicy.Policy()
            policy.set("diffImPolicy.backgroundPolicy.undersample", "ALSO_MAKE_IT_UP")
            policy.mergeDefaults(defPolicy.getDictionary())
        except:
            pass
        else:
            self.fail()
            


class DiffImStageTestCase(unittest.TestCase):
    """A test case for diffimStage.py"""

    def setUp(self):
        defSciencePath = os.path.join(eups.productDir("afwdata"), "DC3a-Sim", "sci", "v26-e0",
                                      "v26-e0-c011-a00.sci")
        defTemplatePath = os.path.join(eups.productDir("afwdata"), "DC3a-Sim", "sci", "v5-e0",
                                       "v5-e0-c011-a00.sci")
        self.scienceExposure   = afwImage.ExposureF(defSciencePath)
        self.templateExposure  = afwImage.ExposureF(defTemplatePath)

    def tearDown(self):
        del self.scienceExposure
        del self.templateExposure

    def subBackground(self, policy):
        # images in afwdata are not background subtracted
        diffimTools.backgroundSubtract(policy, [self.templateExposure.getMaskedImage(),
                                                self.scienceExposure.getMaskedImage()])

    def testSingleExposure(self):
        policyFile = pexPolicy.DefaultPolicyFile("ip_pipeline", 
                                             "DiffImStageDictionary.paf", "policy")
        policy = pexPolicy.Policy.createPolicy(policyFile, policyFile.getRepositoryPath(), True)

        self.subBackground(policy.get("diffImPolicy"))

        stage  = ipPipe.DiffImStage(policy)
        tester = SimpleStageTester(stage)

        #print policy

        clipboard = pexClipboard.Clipboard()
        clipboard.put(policy.get("inputKeys.templateExposureKey"), self.templateExposure)
        clipboard.put(policy.get("inputKeys.scienceExposureKey"), self.scienceExposure)

        outWorker = tester.runWorker(clipboard)
        outPolicy = policy.get("outputKeys")
        
        #print outPolicy
        
        self.assertTrue(outWorker.contains(outPolicy.get("differenceExposureKey")))
        self.assertTrue(outWorker.contains(outPolicy.get("psfMatchingKernelKey")))
        self.assertTrue(outWorker.contains(outPolicy.get("backgroundFunctionKey")))

        # also check types
        diffExposure   = outWorker.get(outPolicy.get("differenceExposureKey"))
        matchingKernel = outWorker.get(outPolicy.get("psfMatchingKernelKey"))
        background     = outWorker.get(outPolicy.get("backgroundFunctionKey"))
        self.assertTrue(isinstance(diffExposure, afwImage.ExposureF))
        self.assertTrue(isinstance(matchingKernel, afwMath.LinearCombinationKernel))
        self.assertTrue(isinstance(background, afwMath.Function2D))

        if display:
            ds9.mtv(outWorker.get(outPolicy.get("differenceExposureKey")), frame=5)

def suite():
    """Returns a suite containing all the test cases in this module."""

    utilsTests.init()

    suites = []

    if not eups.productDir("afwdata"):
        print >> sys.stderr, "afwdata is not setting up; skipping test"
    else:        
        suites += unittest.makeSuite(DiffImStageTestCase)
        pass

    suites += unittest.makeSuite(ValidationTestCase)
    suites += unittest.makeSuite(utilsTests.MemoryTestCase)
    return unittest.TestSuite(suites)

def run(exit=False):
    """Run the tests"""
    utilsTests.run(suite(), exit)

if __name__ == "__main__":
    run(True)

