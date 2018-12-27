import AVFoundation
import Foundation
/// KSAssetExportSession, export and transcode media in Swift
@objc open class KSAssetExportSession: NSObject {
    // private instance vars
    private let inputQueue: DispatchQueue
    private var writer: AVAssetWriter?
    private var reader: AVAssetReader?
    private var pixelBufferAdaptor: AVAssetWriterInputPixelBufferAdaptor?
    private var audioOutput: AVAssetReaderOutput?
    private var progressHandler: ProgressHandler?
    private var renderHandler: RenderHandler?
    private var completionHandler: CompletionHandler?
    private var duration: TimeInterval = 0
    public private(set) var progress: Float = 0
    /// Input asset for export, provided when initialized.
    public var asset: AVAsset?
    public var synchronous = false
    public var writerInput: [AVAssetWriterInput]?
    public var videoOutput: AVAssetReaderOutput?
    /// Enables audio mixing and parameters for the session.
    public var audioMix: AVAudioMix?

    /// Output file location for the session.
    @objc public var outputURL: URL?

    /// Output file type. UTI string defined in `AVMediaFormat.h`.
    @objc public var outputFileType = AVFileType.mp4

    /// Time range or limit of an export from `kCMTimeZero` to `kCMTimePositiveInfinity`
    @objc public var timeRange: CMTimeRange

    /// Indicates if an export session should expect media data in real time.
    @objc public var expectsMediaDataInRealTime = false

    /// Indicates if an export should be optimized for network use.
    @objc public var optimizeForNetworkUse: Bool = false

    @objc public var audioTimePitchAlgorithm: AVAudioTimePitchAlgorithm?
    /// Metadata to be added to an export.
    @objc public var metadata: [AVMetadataItem]?

    /// Video output configuration dictionary, using keys defined in `<AVFoundation/AVVideoSettings.h>`
    @objc public var videoOutputConfiguration: [String: Any]?

    /// Audio output configuration dictionary, using keys defined in `<AVFoundation/AVAudioSettings.h>`
    @objc public var audioOutputConfiguration: [String: Any]?

    /// Export session status state.
    public private(set) var error: Error?
    public var status: AVAssetExportSession.Status {
        if let writer = self.writer {
            switch writer.status {
            case .writing:
                return .exporting
            case .failed:
                return .failed
            case .completed:
                return .completed
            case .cancelled:
                return .cancelled
            case .unknown:
                break
            }
        }
        return .unknown
    }

    // MARK: - object lifecycle

    /// Initializes a session with an asset to export.
    ///
    /// - Parameter asset: The asset to export.
    public convenience init(withAsset asset: AVAsset) {
        self.init()
        self.asset = asset
    }

    override init() {
        timeRange = CMTimeRange(start: .zero, end: .positiveInfinity)
        inputQueue = DispatchQueue(label: "KSSessionExporterInputQueue", target: DispatchQueue.global())
        super.init()
    }

    deinit {
        self.writer = nil
        self.reader = nil
        self.pixelBufferAdaptor = nil
        self.videoOutput = nil
        self.audioOutput = nil
    }
}

// MARK: - export

extension KSAssetExportSession {
    /// Completion handler type for when an export finishes.
    public typealias CompletionHandler = (_ status: AVAssetExportSession.Status, _ erro: Error?) -> Void

    /// Progress handler type
    public typealias ProgressHandler = (_ progress: Float) -> Void

    /// Render handler type for frame processing
    public typealias RenderHandler = (_ renderFrame: CVPixelBuffer, _ presentationTime: CMTime, _ resultingBuffer: CVPixelBuffer) -> Void

    /// Initiates an export session.
    ///
    /// - Parameter completionHandler: Handler called when an export session completes.
    /// - Throws: Failure indication thrown when an error has occurred during export.
    public func export(renderHandler: RenderHandler? = nil, progressHandler: ProgressHandler? = nil, completionHandler: CompletionHandler? = nil) throws {
        cancelExport()
        guard let outputURL = outputURL, let videoOutput = self.videoOutput, let asset = asset, videoOutputConfiguration?.validate() == true else {
            throw NSError(domain: AVFoundationErrorDomain, code: AVError.exportFailed.rawValue, userInfo: [NSLocalizedDescriptionKey: "setup failure"])
        }
        outputURL.remove()
        self.progressHandler = progressHandler
        self.renderHandler = renderHandler
        self.completionHandler = completionHandler
        reader = try AVAssetReader(asset: asset)
        writer = try AVAssetWriter(outputURL: outputURL, fileType: outputFileType)
        guard let reader = reader, let writer = writer else {
            return
        }
        reader.timeRange = timeRange
        writer.shouldOptimizeForNetworkUse = optimizeForNetworkUse
        if let metadata = self.metadata {
            writer.metadata = metadata
        }

        if timeRange.duration.isValid, !timeRange.duration.isPositiveInfinity {
            duration = CMTimeGetSeconds(timeRange.duration)
        } else {
            duration = CMTimeGetSeconds(asset.duration)
        }

        // video
        if reader.canAdd(videoOutput) {
            reader.add(videoOutput)
        }
        guard writer.canApply(outputSettings: videoOutputConfiguration, forMediaType: .video) else {
            throw NSError(domain: AVFoundationErrorDomain, code: AVError.exportFailed.rawValue, userInfo: [NSLocalizedDescriptionKey: "setup failure"])
        }

        let videoInput = AVAssetWriterInput(mediaType: .video, outputSettings: videoOutputConfiguration)
        videoInput.expectsMediaDataInRealTime = expectsMediaDataInRealTime
        if writer.canAdd(videoInput) {
            writer.add(videoInput)
            if renderHandler != nil {
                pixelBufferAdaptor = AVAssetWriterInputPixelBufferAdaptor(assetWriterInput: videoInput, output: videoOutput)
            }
        }

        // audio
        let audioTracks = asset.tracks(withMediaType: .audio)
        if audioTracks.count > 0 {
            let (audioOutput, audioInput) = audioOutputInput(audioTracks: audioTracks)
            if reader.canAdd(audioOutput) {
                reader.add(audioOutput)
                self.audioOutput = audioOutput
                if writer.canAdd(audioInput) {
                    writer.add(audioInput)
                }
            }
        }
        writerInput?.filter { writer.canAdd($0) }.forEach { writer.add($0) }
        // export
        writer.startWriting()
        reader.startReading()
        writer.startSession(atSourceTime: timeRange.start)

        let audioSemaphore = DispatchSemaphore(value: 0)
        let videoSemaphore = DispatchSemaphore(value: 0)

        if let videoInput = writer.inputs.first(where: { $0.mediaType == .video }) {
            videoInput.requestMediaDataWhenReady(on: inputQueue) { [weak self] in
                guard let self = self, self.encode(readySamplesFromReaderOutput: videoOutput, toWriterInput: videoInput) else {
                    videoSemaphore.signal()
                    return
                }
            }
        } else {
            videoSemaphore.signal()
        }

        if let audioOutput = self.audioOutput, let audioInput = writer.inputs.first(where: { $0.mediaType == .audio }) {
            audioInput.requestMediaDataWhenReady(on: inputQueue) { [weak self] in
                guard let self = self, self.encode(readySamplesFromReaderOutput: audioOutput, toWriterInput: audioInput) else {
                    audioSemaphore.signal()
                    return
                }
            }
        } else {
            audioSemaphore.signal()
        }
        let finish = {
            audioSemaphore.wait()
            videoSemaphore.wait()
            DispatchQueue.main.sync { [weak self] in self?.finish() }
        }
        if synchronous {
            finish()
        } else {
            DispatchQueue.global().async { finish() }
        }
    }

    /// Cancels any export in progress.
    @objc public func cancelExport() {
        guard writer != nil || reader != nil else {
            return
        }
        inputQueue.async { [weak self] in
            guard let self = self else {
                return
            }
            if self.writer?.status == .writing {
                self.writer?.cancelWriting()
            }
            if self.reader?.status == .reading {
                self.reader?.cancelReading()
            }
            self.complete()
            self.reset()
        }
    }
}

// MARK: - private funcs

extension KSAssetExportSession {
    private func audioOutputInput(audioTracks: [AVAssetTrack]) -> (AVAssetReaderOutput, AVAssetWriterInput) {
        let audioOutput = AVAssetReaderAudioMixOutput(audioTracks: audioTracks, audioSettings: nil)
        audioOutput.alwaysCopiesSampleData = false
        if let audioTimePitchAlgorithm = audioTimePitchAlgorithm {
            audioOutput.audioTimePitchAlgorithm = audioTimePitchAlgorithm
        }
        audioOutput.audioMix = audioMix
        let audioInput = AVAssetWriterInput(mediaType: .audio, outputSettings: audioOutputConfiguration)
        audioInput.expectsMediaDataInRealTime = expectsMediaDataInRealTime
        return (audioOutput, audioInput)
    }

    // called on the inputQueue
    private func encode(readySamplesFromReaderOutput output: AVAssetReaderOutput, toWriterInput input: AVAssetWriterInput) -> Bool {
        while input.isReadyForMoreMediaData {
            guard let sampleBuffer = output.copyNextSampleBuffer() else {
                input.markAsFinished()
                return false
            }
            guard reader?.status == .reading, writer?.status == .writing else {
                return false
            }
            if output.mediaType == .video {
                let lastSamplePresentationTime = CMSampleBufferGetPresentationTimeStamp(sampleBuffer) - timeRange.start
                let progress = duration == 0 ? 1 : Float(lastSamplePresentationTime.seconds / duration)
                updateProgress(progress: progress)
                if let renderHandler = renderHandler, let pixelBufferAdaptor = pixelBufferAdaptor, let pixelBufferPool = pixelBufferAdaptor.pixelBufferPool, let pixelBuffer = CMSampleBufferGetImageBuffer(sampleBuffer) {
                    var toRenderBuffer: CVPixelBuffer?
                    let result = CVPixelBufferPoolCreatePixelBuffer(kCFAllocatorDefault, pixelBufferPool, &toRenderBuffer)
                    if result == kCVReturnSuccess, let toBuffer = toRenderBuffer {
                        renderHandler(pixelBuffer, lastSamplePresentationTime, toBuffer)
                        if !pixelBufferAdaptor.append(toBuffer, withPresentationTime: lastSamplePresentationTime) {
                            return false
                        }
                    }
                }
            }
            guard input.append(sampleBuffer) else {
                return false
            }
        }
        return true
    }

    private func updateProgress(progress: Float) {
        willChangeValue(forKey: "progress")
        self.progress = progress
        didChangeValue(forKey: "progress")
        progressHandler?(progress)
    }

    private func finish() {
        if reader?.status == .cancelled || writer?.status == .cancelled {
            return
        }
        error = writer?.error ?? reader?.error
        if writer?.status == .failed {
            complete()
        } else if reader?.status == .failed {
            writer?.cancelWriting()
            complete()
        } else {
            writer?.finishWriting {
                self.complete()
            }
        }
    }

    private func complete() {
        if status == .failed || status == .cancelled {
            outputURL?.remove()
        }
        completionHandler?(status, error)
        completionHandler = nil
    }

    private func reset() {
        progress = 0
        writer = nil
        reader = nil
        pixelBufferAdaptor = nil
        videoOutput = nil
        audioOutput = nil
        progressHandler = nil
        renderHandler = nil
        completionHandler = nil
    }
}

// MARK: - AVAsset extension

extension AVAsset {
    /// Initiates a NextLevelSessionExport on the asset
    ///
    /// - Parameters:
    ///   - outputFileType: type of resulting file to create
    ///   - outputURL: location of resulting file
    ///   - metadata: data to embed in the result
    ///   - videoInputConfiguration: video input configuration
    ///   - videoOutputConfiguration: video output configuration
    ///   - audioOutputConfiguration: audio output configuration
    ///   - progressHandler: progress fraction handler
    ///   - completionHandler: completion handler
    public func export(outputFileType: AVFileType = AVFileType.mp4,
                       outputURL: URL,
                       metadata _: [AVMetadataItem]? = nil,
                       videoInputConfiguration: [String: Any]? = nil,
                       videoOutputConfiguration: [String: Any],
                       audioOutputConfiguration: [String: Any],
                       progressHandler: KSAssetExportSession.ProgressHandler? = nil,
                       completionHandler: KSAssetExportSession.CompletionHandler? = nil) throws {
        let exporter = KSAssetExportSession(withAsset: self)
        exporter.outputFileType = outputFileType
        exporter.outputURL = outputURL
        exporter.videoOutputConfiguration = videoOutputConfiguration
        exporter.audioOutputConfiguration = audioOutputConfiguration
        let videoOutput = AVAssetReaderVideoCompositionOutput(videoTracks: tracks(withMediaType: .video), videoSettings: videoInputConfiguration)
        videoOutput.alwaysCopiesSampleData = false
        videoOutput.videoComposition = makeVideoComposition(videoOutputConfiguration: videoOutputConfiguration)
        exporter.videoOutput = videoOutput
        try exporter.export(progressHandler: progressHandler, completionHandler: completionHandler)
    }

    public func quickTimeMov(outputURL: URL, assetIdentifier: String,
                             completionHandler: KSAssetExportSession.CompletionHandler? = nil) throws {
        guard let videoTrack = tracks(withMediaType: .video).first else {
            return
        }
        let exporter = KSAssetExportSession(withAsset: self)
        exporter.outputFileType = .mov
        exporter.outputURL = outputURL
        exporter.videoOutput = AVAssetReaderTrackOutput(track: videoTrack, outputSettings: [kCVPixelBufferPixelFormatTypeKey as String: NSNumber(value: kCVPixelFormatType_32BGRA)])
        exporter.videoOutputConfiguration = [
            AVVideoCodecKey: AVVideoCodecH264 as AnyObject,
            AVVideoWidthKey: videoTrack.naturalSize.width as AnyObject,
            AVVideoHeightKey: videoTrack.naturalSize.height as AnyObject,
        ]
        exporter.metadata = [AVMutableMetadataItem(assetIdentifier: assetIdentifier)]
        exporter.writerInput = [AVAssetWriterInput.makeMetadataAdapter()]
        exporter.synchronous = true
        try exporter.export(progressHandler: nil, completionHandler: completionHandler)
    }

    fileprivate func makeVideoComposition(videoOutputConfiguration: [String: Any]?) -> AVMutableVideoComposition {
        let videoComposition = AVMutableVideoComposition()
        guard let videoTrack = tracks(withMediaType: .video).first else {
            return videoComposition
        }
        // determine the framerate
        var frameRate: Float = 24
        if let videoCompressionConfiguration = videoOutputConfiguration?[AVVideoCompressionPropertiesKey] as? [String: Any] {
            if let trackFrameRate = videoCompressionConfiguration[AVVideoExpectedSourceFrameRateKey] as? NSNumber {
                frameRate = trackFrameRate.floatValue
            }
        } else {
            frameRate = videoTrack.nominalFrameRate
        }
        videoComposition.frameDuration = CMTimeMake(value: 1, timescale: Int32(frameRate))
        // determine the appropriate size and transform
        if let videoConfiguration = videoOutputConfiguration {
            let videoWidth = videoConfiguration[AVVideoWidthKey] as? NSNumber
            let videoHeight = videoConfiguration[AVVideoHeightKey] as? NSNumber
            let targetSize = CGSize(width: videoWidth!.intValue, height: videoHeight!.intValue)
            var naturalSize = videoTrack.naturalSize
            var transform = videoTrack.preferredTransform
            if transform.ty == -560 {
                transform.ty = 0
            }

            if transform.tx == -560 {
                transform.tx = 0
            }
            let videoAngleInDegrees = atan2(transform.b, transform.a) * 180 / .pi
            if videoAngleInDegrees == 90 || videoAngleInDegrees == -90 {
                naturalSize = CGSize(width: naturalSize.height, height: naturalSize.width)
            }
            videoComposition.renderSize = naturalSize

            // center the video
            var ratio: CGFloat = 0
            let xRatio: CGFloat = targetSize.width / naturalSize.width
            let yRatio: CGFloat = targetSize.height / naturalSize.height
            ratio = min(xRatio, yRatio)
            let postWidth = naturalSize.width * ratio
            let postHeight = naturalSize.height * ratio
            let transX = (targetSize.width - postWidth) * 0.5
            let transY = (targetSize.height - postHeight) * 0.5

            var matrix = CGAffineTransform(translationX: (transX / xRatio), y: (transY / yRatio))
            matrix = matrix.scaledBy(x: (ratio / xRatio), y: (ratio / yRatio))
            transform = transform.concatenating(matrix)
            // make the composition
            let compositionInstruction = AVMutableVideoCompositionInstruction()
            compositionInstruction.timeRange = CMTimeRange(start: .zero, duration: duration)

            let layerInstruction = AVMutableVideoCompositionLayerInstruction(assetTrack: videoTrack)
            layerInstruction.setTransform(transform, at: .zero)
            compositionInstruction.layerInstructions = [layerInstruction]
            videoComposition.instructions = [compositionInstruction]
        }
        return videoComposition
    }
}

extension AVAssetWriterInput {
    fileprivate static func makeMetadataAdapter() -> AVAssetWriterInput {
        let spec = [
            kCMMetadataFormatDescriptionMetadataSpecificationKey_Identifier:
                "mdta/com.apple.quicktime.still-image-time",
            kCMMetadataFormatDescriptionMetadataSpecificationKey_DataType:
                "com.apple.metadata.datatype.int8",
        ]
        var desc: CMFormatDescription?
        CMMetadataFormatDescriptionCreateWithMetadataSpecifications(allocator: kCFAllocatorDefault, metadataType: kCMMetadataFormatType_Boxed, metadataSpecifications: [spec] as CFArray, formatDescriptionOut: &desc)
        let input = AVAssetWriterInput(mediaType: .metadata, outputSettings: nil, sourceFormatHint: desc)
        let adapter = AVAssetWriterInputMetadataAdaptor(assetWriterInput: input)
        adapter.append(AVTimedMetadataGroup(items: [AVMutableMetadataItem.makeStillImageTime()], timeRange: CMTimeRange(start: CMTime(value: 0, timescale: 1000), duration: CMTime(value: 200, timescale: 3000))))
        return adapter.assetWriterInput
    }
}

extension AVMutableMetadataItem {
    fileprivate convenience init(assetIdentifier: String) {
        self.init()
        key = "com.apple.quicktime.content.identifier" as NSCopying & NSObjectProtocol
        keySpace = AVMetadataKeySpace(rawValue: "mdta")
        value = assetIdentifier as NSCopying & NSObjectProtocol
        dataType = "com.apple.metadata.datatype.UTF-8"
    }

    fileprivate static func makeStillImageTime() -> AVMutableMetadataItem {
        let item = AVMutableMetadataItem()
        item.key = "com.apple.quicktime.still-image-time" as NSCopying & NSObjectProtocol
        item.keySpace = AVMetadataKeySpace(rawValue: "mdta")
        item.value = -1 as NSCopying & NSObjectProtocol
        item.dataType = "com.apple.metadata.datatype.int8"
        return item
    }
}

extension Dictionary where Key == String, Value == Any {
    fileprivate mutating func validate() -> Bool {
        if !keys.contains(AVVideoCodecKey) {
            debugPrint("KSAssetExportSession, warning a video output configuration codec wasn't specified")
            if #available(iOS 11.0, *) {
                self[AVVideoCodecKey] = AVVideoCodecType.h264
            } else {
                self[AVVideoCodecKey] = AVVideoCodecH264
            }
        }
        let videoWidth = self[AVVideoWidthKey] as? NSNumber
        let videoHeight = self[AVVideoHeightKey] as? NSNumber
        if videoWidth == nil || videoHeight == nil {
            return false
        }
        return true
    }
}

extension URL {
    fileprivate func remove() {
        if FileManager.default.fileExists(atPath: absoluteString) {
            do {
                try FileManager.default.removeItem(at: self)
            } catch {
                debugPrint("KSAssetExportSession, failed to delete file at \(self)")
            }
        }
    }
}

extension AVAssetWriterInputPixelBufferAdaptor {
    public convenience init(assetWriterInput input: AVAssetWriterInput, output: AVAssetReaderOutput) {
        var pixelBufferAttrib: [String: Any] = [
            kCVPixelBufferPixelFormatTypeKey as String: NSNumber(integerLiteral: Int(kCVPixelFormatType_32RGBA)),
            "IOSurfaceOpenGLESTextureCompatibility": NSNumber(booleanLiteral: true),
            "IOSurfaceOpenGLESFBOCompatibility": NSNumber(booleanLiteral: true),
        ]
        if let videoOutput = output as? AVAssetReaderVideoCompositionOutput, let videoComposition = videoOutput.videoComposition {
            pixelBufferAttrib[kCVPixelBufferWidthKey as String] = NSNumber(integerLiteral: Int(videoComposition.renderSize.width))
            pixelBufferAttrib[kCVPixelBufferHeightKey as String] = NSNumber(integerLiteral: Int(videoComposition.renderSize.height))
        }
        self.init(assetWriterInput: input, sourcePixelBufferAttributes: pixelBufferAttrib)
    }
}
