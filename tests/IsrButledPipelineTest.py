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
from lsst.pex.harness import Dataset
import lsst.pex.harness.IOStage
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

def foo():
    afwDataDir = eups.productDir("afwdata")
    obsDir = eups.productDir('obs_lsstSim')
    isrDir = eups.productDir('ip_isr')
    pipelineDir = eups.productDir('ip_pipeline')
    lutpolicypath = os.path.join(isrDir,"pipeline")
    pipepolicypath = os.path.join(pipelineDir,"policy")
    dc3MetadataFile = 'dc3MetadataPolicy.paf'
    simDatatypeFile = 'simDataTypePolicy.paf'
    suffix = "Keyword"

    """Pipeline test case."""
    clipboard = pexClipboard.Clipboard()         
    clipboard.put('jobIdentity', {
            'visit': 85751839, 'snap': 0,
            'raft': "R:2,3", 'sensor': "S:1,1", 'channel': "00"
        })
    clipboard.put('inputDatasets', [
            Dataset("raw", visit=85751839, snap=0,
                raft="R:2,3", sensor="S:1,1", channel="00")
        ])
    clipboard.put("fwhm", 5.)
    p0 = pexPolicy.Policy.createPolicy("IsrInputStage.paf")
    s0 = lsst.pex.harness.IOStage.InputStage(p0)
    t0 = SimpleStageTester(s0)

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

    #These are guesses to adhere to legacy policy files
    #In the future these values should not be passed on the 
    #clipboard.
    #TODO get exposure times directly from exposure metadata
    clipboard.put(p5.get("inputKeys.exposurescale"),
            15)
    clipboard.put(p5.get("inputKeys.darkscale"),
            60)

    file = pexPolicy.DefaultPolicyFile("ip_pipeline", 
                                       "IsrFlatStageDictionary.paf",
                                       "policy")
    p6 = pexPolicy.Policy.createPolicy(file)
    s6 = ipPipe.IsrFlatStage(p6)
    t6 = SimpleStageTester(s6)
    clipboard.put(p6.get("inputKeys.flatscalingtype"), 'MEAN')
    o0 = t0.runWorker(clipboard)
    o2 = t2.runWorker(o0)
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
    exposure = o6.get(p6.get("outputKeys.flatCorrectedExposure"))
    if display:
        ds9.mtv(exposure, frame=0, title="Output")

if __name__=="__main__":
    foo()
