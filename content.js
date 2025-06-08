// content.js
// This script is injected into the active tab.
// It handles displaying subtitles and initiating/processing tab audio capture.

console.log("Content script loaded.");

// --- Subtitle Display Elements ---
var subtitleContainer = null;
var japaneseSubtitleElement = null;
var chineseSubtitleElement = null;

// --- Audio Capture Variables ---
var audioContext = null;
var mediaStreamSource = null;
var audioProcessor = null;
var audioStream = null;
var isCapturing = false;

// IMPORTANT: Sample rate for AWS Transcribe
var TARGET_SAMPLE_RATE = 16000;

// --- Audio Resampling Variables ---
var offlineContext = null;
var resampledBuffer = null;

// --- Subtitle Display Logic (Same as before) ---
function createSubtitleElements() {
  // Check if elements already exist (more robust check using ID)
  if (document.getElementById('chrome-stt-translate-subtitle-container')) {
     subtitleContainer = document.getElementById('chrome-stt-translate-subtitle-container');
     japaneseSubtitleElement = subtitleContainer.querySelector('.stt-translate-subtitle-japanese');
     chineseSubtitleElement = subtitleContainer.querySelector('.stt-translate-subtitle-chinese');
     console.log("Subtitle elements already exist, reusing.");
     return;
  }

  console.log("Creating subtitle elements.");
  subtitleContainer = document.createElement('div');
  subtitleContainer.classList.add('stt-translate-subtitle-container');
  subtitleContainer.id = 'chrome-stt-translate-subtitle-container'; // Add a unique ID

  japaneseSubtitleElement = document.createElement('div');
  japaneseSubtitleElement.classList.add('stt-translate-subtitle-japanese');
  subtitleContainer.appendChild(japaneseSubtitleElement);

  chineseSubtitleElement = document.createElement('div');
  chineseSubtitleElement.classList.add('stt-translate-subtitle-chinese');
  subtitleContainer.appendChild(chineseSubtitleElement);

  document.body.appendChild(subtitleContainer);
  console.log("Subtitle container added to body.");
}

function updateSubtitles(japaneseText, chineseText) {
  // Ensure elements exist before updating
  if (!subtitleContainer) {
    createSubtitleElements(); // Should ideally be created by startContentScript message
    if (!subtitleContainer) { // If creation still fails for some reason
         console.error("Failed to create subtitle elements.");
         return;
    }
  }
  // Update text content, handle null/undefined gracefully
  japaneseSubtitleElement.textContent = japaneseText || '';
  chineseSubtitleElement.textContent = chineseText || '';
}

function removeSubtitleElements() {
  const existingContainer = document.getElementById('chrome-stt-translate-subtitle-container');
  if (existingContainer && existingContainer.parentNode) {
    console.log("Removing subtitle elements.");
    existingContainer.parentNode.removeChild(existingContainer);
  }
  // Reset variables regardless of whether element was found/removed
  subtitleContainer = null;
  japaneseSubtitleElement = null;
  chineseSubtitleElement = null;
  console.log("Subtitle elements removed (if they existed).");
}

// --- Audio Capture and Processing Logic ---
async function initiateTabCapture() {
    console.log("Initiating tab capture...");

    // Stop any existing capture first
    await stopAudioCapture();

    try {
        const { streamId, error } = await chrome.runtime.sendMessage({ action: "getStreamId" });
        if (!streamId) {
            throw new Error(error || "Failed to obtain stream ID");
        }

        const stream = await navigator.mediaDevices.getUserMedia({
            audio: {
                mandatory: {
                    chromeMediaSource: "tab",
                    chromeMediaSourceId: streamId
                }
            },
            video: false
        });

        console.log("Tab capture successful. Stream received.");
        audioStream = stream;
        isCapturing = true;
        await processAudioStream(audioStream);
    } catch (error) {
        console.error("Tab capture failed:", error);
        chrome.runtime.sendMessage({ 
            action: "audioCaptureError", 
            error: `Tab capture failed: ${error.message}` 
        }).catch(e => console.error("Error sending audioCaptureError message:", e));
        await stopAudioCapture();
    }
}

async function processAudioStream(stream) {
    console.log("Processing audio stream with Web Audio API...");
    try {
        audioContext = new (window.AudioContext || window.webkitAudioContext)();
        console.log(`AudioContext sample rate: ${audioContext.sampleRate} Hz`);
        
        mediaStreamSource = audioContext.createMediaStreamSource(stream);
        
        // Create a new AudioWorkletNode for modern audio processing
        await audioContext.audioWorklet.addModule(URL.createObjectURL(new Blob([`
            class AudioProcessor extends AudioWorkletProcessor {
                constructor() {
                    super();
                    this.bufferSize = 4096;
                    this.buffer = new Float32Array(this.bufferSize);
                    this.bufferIndex = 0;
                }
                
                process(inputs, outputs, parameters) {
                    const input = inputs[0][0];
                    if (!input) return true;
                    
                    for (let i = 0; i < input.length; i++) {
                        this.buffer[this.bufferIndex++] = input[i];
                        
                        if (this.bufferIndex >= this.bufferSize) {
                            this.port.postMessage({
                                audioData: this.buffer.slice(0)
                            });
                            this.bufferIndex = 0;
                        }
                    }
                    return true;
                }
            }
            registerProcessor('audio-processor', AudioProcessor);
        `], { type: 'application/javascript' })));

        const workletNode = new AudioWorkletNode(audioContext, 'audio-processor');
        // Expose the node globally so it can be disconnected later
        audioProcessor = workletNode;
        
        workletNode.port.onmessage = async (event) => {
            if (!isCapturing) return;
            
            const audioData = event.data.audioData;
            const resampledData = await resampleAudio(audioData, audioContext.sampleRate, TARGET_SAMPLE_RATE);
            const int16Data = convertFloat32ToInt16(resampledData);
            
            chrome.runtime.sendMessage({ 
                action: "audioChunk", 
                chunk: int16Data.buffer 
            }).catch(e => console.error("Error sending audio chunk:", e));
        };

        mediaStreamSource.connect(workletNode);
        workletNode.connect(audioContext.destination);
        
        console.log("Audio processing graph connected. Sending audio chunks...");
        chrome.runtime.sendMessage({ action: "audioProcessingStarted" })
            .catch(e => console.error("Error sending audioProcessingStarted message:", e));
    } catch (error) {
        console.error("Error setting up audio processing:", error);
        chrome.runtime.sendMessage({ 
            action: "audioCaptureError", 
            error: `Audio processing setup failed: ${error.message}` 
        }).catch(e => console.error("Error sending audioCaptureError message:", e));
        await stopAudioCapture();
    }
}

async function resampleAudio(audioData, fromSampleRate, toSampleRate) {
    if (fromSampleRate === toSampleRate) {
        return audioData;
    }

    const ratio = toSampleRate / fromSampleRate;
    const newLength = Math.round(audioData.length * ratio);
    const result = new Float32Array(newLength);
    
    // Linear interpolation resampling
    for (let i = 0; i < newLength; i++) {
        const position = i / ratio;
        const index = Math.floor(position);
        const fraction = position - index;
        
        const value = audioData[index];
        const nextValue = audioData[Math.min(index + 1, audioData.length - 1)];
        
        result[i] = value + fraction * (nextValue - value);
    }
    
    return result;
}

async function stopAudioCapture() {
    console.log("Stopping audio capture and processing...");
    isCapturing = false;

    if (audioProcessor) {
        try {
            audioProcessor.port.onmessage = null;
            audioProcessor.disconnect();
            console.log("Audio processor disconnected.");
        } catch (e) {
            console.error("Error disconnecting audio processor:", e);
        }
        audioProcessor = null;
    }
    
    if (mediaStreamSource) {
        mediaStreamSource.disconnect();
        mediaStreamSource = null;
    }
    
    if (audioContext) {
        try {
            await audioContext.close();
            console.log("AudioContext closed.");
        } catch (e) {
            console.error("Error closing AudioContext:", e);
        }
        audioContext = null;
    }
    
    if (audioStream) {
        audioStream.getTracks().forEach(track => track.stop());
        console.log("Audio stream tracks stopped.");
        audioStream = null;
    }
    
    console.log("Audio capture and processing stopped.");
}

function convertFloat32ToInt16(buffer) {
    const l = buffer.length;
    const buf = new Int16Array(l);
    const multiplier = 0x7FFF;
    
    for (let i = 0; i < l; i++) {
        const s = Math.max(-1, Math.min(1, buffer[i]));
        buf[i] = Math.round(s * multiplier);
    }
    
    return buf;
}

// --- Message Handling from Background Script ---

// Listen for messages from the background service worker
chrome.runtime.onMessage.addListener((request, sender, sendResponse) => {
  console.log("Content script received message:", request.action, sender.tab ? "from background script for tab " + sender.tab.id : "from popup/other");

  // Handle messages from the background script
  if (request.action === "startContentScript") {
    // Received initial signal to set up subtitles
    console.log("Received startContentScript signal. Setting up subtitles.");
    createSubtitleElements();
    // We no longer initiate capture immediately here.
    // Capture will be initiated by a separate message (initiateTabCapture).
    sendResponse({ success: true }); // Acknowledge message

  } else if (request.action === "initiateTabCapture") {
      // Received signal from background to actually start tab capture (Task 2.2)
      console.log("Received initiateTabCapture signal.");
      initiateTabCapture(); // Call the function to start capture
      sendResponse({ success: true }); // Acknowledge message

  } else if (request.action === "stopContentScript") {
    // Received signal to stop the process
    console.log("Received stop signal. Removing subtitles and stopping capture.");
    removeSubtitleElements();
    stopAudioCapture(); // Stop capture and processing
    sendResponse({ success: true }); // Acknowledge message

  } else if (request.action === "updateSubtitles") {
    // Received transcribed Japanese and translated Chinese text
    // console.log("Received subtitle update:", request.japanese, request.chinese); // Log can be noisy
    updateSubtitles(request.japanese, request.chinese);
    sendResponse({ success: true }); // Acknowledge message

  } else if (request.action === "awsReady") {
      // Optional: Received signal that AWS WebSocket is open and ready
      console.log("Background script reports AWS WebSocket is ready.");
      // If you delayed audio capture until AWS was ready, start it here:
      // initiateTabCapture(); // Uncomment this line if you want to wait for AWS readiness
      sendResponse({ success: true });
  } else if (request.action === "sttError" || request.action === "translateError" || request.action === "audioCaptureError") {
      // Received an error message from the background script or self-reported capture error
      console.error(`Received error from background/self (${request.action}):`, request.error);
      // Display the error using the subtitle area
      updateSubtitles("", `[服务出错: ${request.error}]`);
      // Optionally stop processing automatically on persistent errors
      // stopAudioCapture(); // Might already be called by initiateTabCapture error handler or background script
      sendResponse({ success: true }); // Acknowledge message
  }
  // Add other message handlers if needed
});

// Optional: Initial setup check - Less critical with explicit start/stop
// chrome.runtime.sendMessage({ action: "getStatus" }, function(response) {
//   if (response && response.isRunning) {
//     console.log("Background reports running on load. Setting up content script.");
//     createSubtitleElements();
//     // Note: Capture is NOT automatically started here. It's initiated by a separate message.
//   } else {
//       console.log("Background reports idle on load.");
//   }
// });

console.log("Content script finished execution.");