//
import AVFoundation
//  CompressTest.swift
//  KSAssetExportSession
//
//  Created by kintan on 2018/12/26.
//
@testable import KSAssetExportSession
import XCTest

class CompressTest: XCTestCase {
    private var expectation: XCTestExpectation?

    override func setUp() {
        // Put setup code here. This method is called before the invocation of each test method in the class.
    }

    override func tearDown() {
        // Put teardown code here. This method is called after the invocation of each test method in the class.
    }

    func testExample() {
        let asset = AVAsset(url: Bundle(for: type(of: self)).url(forResource: "test", withExtension: "MOV")!)
        let tmpURL = URL(fileURLWithPath: NSTemporaryDirectory(), isDirectory: true)
            .appendingPathComponent(ProcessInfo().globallyUniqueString)
            .appendingPathExtension("mp4")
        let compressionDict: [String: Any] = [
            AVVideoAverageBitRateKey: NSNumber(integerLiteral: 6_000_000),
            AVVideoProfileLevelKey: AVVideoProfileLevelH264HighAutoLevel as String,
        ]
        let videoOutputConfiguration = [
            AVVideoCodecKey: AVVideoCodecH264,
            AVVideoWidthKey: NSNumber(integerLiteral: 540),
            AVVideoHeightKey: NSNumber(integerLiteral: 960),
            AVVideoScalingModeKey: AVVideoScalingModeResizeAspectFill,
            AVVideoCompressionPropertiesKey: compressionDict,
        ] as [String: Any]
        let audioOutputConfiguration = [
            AVFormatIDKey: kAudioFormatMPEG4AAC,
            AVEncoderBitRateKey: NSNumber(integerLiteral: 128_000),
            AVNumberOfChannelsKey: NSNumber(integerLiteral: 2),
            AVSampleRateKey: NSNumber(value: Float(44100)),
        ] as [String: Any]

        expectation = expectation(description: "compress")
        do {
            try asset.export(outputURL: tmpURL, videoOutputConfiguration: videoOutputConfiguration, audioOutputConfiguration: audioOutputConfiguration, progressHandler: { progress in
                print(progress)
            }) { status, error in
                switch status {
                case .completed:
                    self.expectation?.fulfill()
                    print("SessionExporter, export completed, \(tmpURL.description)")
                    break
                case .cancelled:
                    print("SessionExporter, export cancelled")
                    break
                case .failed:
                    print("SessionExporter, failed to export, \(error.debugDescription)")
                    break
                case .exporting:
                    fallthrough
                case .waiting:
                    fallthrough
                default:
                    print("SessionExporter, did not complete")
                    break
                }
            }
        } catch {
            print("SessionExporter, failed to export")
        }
        waitForExpectations(timeout: 20) { _ in
        }
    }

    func testPerformanceExample() {
        // This is an example of a performance test case.
        measure {
//            (0..<1000).forEach {_ in
            testExample()
//            }
        }
    }
}
