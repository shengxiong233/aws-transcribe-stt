// background.js

// --- AWS SDK Imports ---
// These imports are necessary for SigV4 signing, Event Stream marshalling/unmarshalling,
// and the Translate Client. Ensure your bundler (Webpack) resolves these correctly.
import { TranscribeStreamingClient } from "@aws-sdk/client-transcribe"; // Keep for potential type hints or future use
import { TranslateClient, TranslateTextCommand } from "@aws-sdk/client-translate";
import { SignatureV4 } from "@aws-sdk/signature-v4";
import { Sha256 } from "@aws-crypto/sha256-js";
import { parseUrl } from "@aws-sdk/url-parser";
import { buildHttpRequest, HttpRequest } from "@aws-sdk/protocol-http";
import { EventStreamMarshaller } from "@aws-sdk/eventstream-marshaller";
import { fromUtf8, toUtf8 } from "@aws-sdk/util-utf8-node"; // Provided by polyfill/bundler

console.log("Background service worker starting...");

// --- State ---
let isRunning = false;
let awsConfig = null;
let transcribeWebSocket = null;
let translateClient = null; // AWS Translate Client instance
let currentTabId = null; // Keep track of the tab where processing is active

// --- Configuration ---
const TRANSCRIBE_LANGUAGE_CODE = "ja-JP"; // Japanese for STT
const TRANSLATE_SOURCE_LANGUAGE = "ja"; // Japanese for Translate
const TRANSLATE_TARGET_LANGUAGE = "zh"; // Chinese for Translate
// IMPORTANT: Sample rate MUST match the sample rate of the audio chunks received from content.js
// If content.js captures at 44100Hz and doesn't resample, you must set this to 44100.
// If content.js resamples to 16000Hz, set this to 16000.
const AUDIO_CHUNK_SAMPLE_RATE = 16000; // Assuming content.js resamples or captures at this rate (Task 2.4)
const TRANSCRIBE_SERVICE_NAME = 'transcribe'; // AWS service name for Transcribe

// --- Helper: Get AWS Credentials (Same as before) ---
async function getAwsCredentials() {
  return new Promise((resolve, reject) => {
    chrome.storage.sync.get(['awsAccessKeyId', 'awsSecretAccessKey', 'awsRegion'], function(data) {
      if (data.awsAccessKeyId && data.awsSecretAccessKey && data.awsRegion) {
        awsConfig = {
          accessKeyId: data.awsAccessKeyId,
          secretAccessKey: data.awsSecretAccessKey,
          region: data.awsRegion
        };
        console.log("AWS Credentials loaded from storage.");
        resolve(awsConfig);
      } else {
        console.warn("AWS Credentials not found in storage.");
        reject("Credentials not configured");
      }
    });
  });
}

// --- AWS Transcribe Real-time WebSocket Logic (Task 1.2, 1.3, 1.4) ---
async function startTranscribeWebSocket(config) {
  console.log("Attempting to connect to AWS Transcribe...");

  const endpoint = `wss://transcribestreaming.${config.region}.amazonaws.com:8443`;
  const url = parseUrl(endpoint); // Use parseUrl from SDK

  // --- SigV4 Signing Implementation (Task 1.2) ---
  // 1. Create a signer instance
  const signer = new SignatureV4({
    credentials: { // Provide AWS credentials
        accessKeyId: config.accessKeyId,
        secretAccessKey: config.secretAccessKey,
        // sessionToken: config.sessionToken, // Include if using temporary credentials
    },
    region: config.region, // AWS region
    service: TRANSCRIBE_SERVICE_NAME, // Service name
    sha256: Sha256, // Hashing function (Requires @aws-crypto/sha256-js)
  });

  // 2. Create an HttpRequest object representing the WebSocket handshake GET request
  // These parameters are required by the Transcribe streaming API.
  const request = new HttpRequest({
      method: 'GET', // WebSocket handshake starts with a GET request
      hostname: url.hostname,
      path: '/stream-transcription-websocket', // <--- Correct path based on AWS format
      query: { // Query parameters required by Transcribe streaming API
          'language-code': TRANSCRIBE_LANGUAGE_CODE,
          'media-encoding': 'pcm', // Must match the format of the audio chunks sent (16-bit PCM)
          'sample-rate': AUDIO_CHUNK_SAMPLE_RATE.toString(), // Must match the sample rate of the audio chunks
          // Add other optional parameters here if needed, e.g.:
          // 'show-speaker-label': 'true',
          // 'enable-partial-results-stabilization': 'true',
          // 'partial-results-stability': 'high', // 'low', 'medium', 'high'
          // 'vocabulary-name': 'YourVocabularyName',
      },
      protocol: url.protocol,
      // Add Host header explicitly - required for signing robustness
      headers: { Host: url.hostname },
  });

  // 3. Sign the request
  // The signing process adds Authorization, x-amz-date, etc. headers
  // expiresIn is in seconds (e.g., 300s = 5 minutes). Max is 7 days (604800 seconds).
  const signedRequest = await signer.sign(request, { signingDate: new Date(), expiresIn: 300 });

  // 4. Build the final WebSocket URL
  // The signed headers need to be transferred to query parameters for the WebSocket handshake.
  // Combine original query parameters and signed headers.
  const params = new URLSearchParams(signedRequest.query); // Start with original query params
  Object.entries(signedRequest.headers).forEach(([key, value]) => {
       // Add relevant signed headers as query parameters.
       // Use lowercase keys from signedRequest.headers as they are canonical.
       if (key.toLowerCase() === 'authorization' || key.toLowerCase() === 'x-amz-date' || key.toLowerCase() === 'x-amz-security-token' || key.toLowerCase() === 'host') { // Include host header
           params.append(key, value);
       }
  });

  // Construct the final WebSocket URL
  const signedUrl = `wss://${signedRequest.hostname}${signedRequest.path}?${params.toString()}`;

  console.log("Signed WebSocket URL constructed.");
  console.log("Full Signed URL (for debugging):", signedUrl); // Keep this log for debugging 403/404
  console.log("Signed Headers (for debugging):", signedRequest.headers); // Log headers too


  // 5. Create the WebSocket instance using the signed URL
  transcribeWebSocket = new WebSocket(signedUrl);

  // --- End SigV4 Signing Implementation ---


  // Event Stream Marshaller (Task 1.3/1.4)
  const eventMarshaller = new EventStreamMarshaller(toUtf8, fromUtf8); // Requires @aws-sdk/eventstream-marshaller, util-utf8-node

  transcribeWebSocket.onopen = (event) => {
    console.log("Transcribe WebSocket opened successfully.");
    // Send initial settings message (required by Transcribe event stream protocol)
    // This message format is specific to Transcribe event stream.
    // It confirms configuration like language, encoding, sample rate.
    // Implementation (Task 1.3/1.4):
    try {
        const greetingMessage = {
            headers: {
                ':message-type': { type: 'string', value: 'event' },
                ':event-type': { type: 'string', value: 'configuration-event' },
            },
            body: JSON.stringify({
                LanguageCode: TRANSCRIBE_LANGUAGE_CODE,
                MediaEncoding: 'pcm', // Must match audio format sent from content.js
                SampleRate: AUDIO_CHUNK_SAMPLE_RATE, // Must match audio sample rate sent from content.js
                // Add other configuration options here if needed
            }),
        };
        const binaryMessage = eventMarshaller.marshall(greetingMessage);
        transcribeWebSocket.send(binaryMessage);
        console.log("Sent Transcribe configuration message.");
    } catch (error) {
        console.error("Error sending Transcribe configuration message:", error);
        // Handle error, maybe close socket?
        updateSubtitlesInContentScript("", `[STT配置出错: ${error.message}]`);
        stopProcessing();
    }

     console.log("WebSocket opened. Ready to receive audio data.");
     // Inform content script that AWS is ready to receive audio (optional but good practice)
     if (currentTabId) {
         chrome.tabs.sendMessage(currentTabId, { action: "awsReady" }).catch(e => console.error("Error sending awsReady to tab:", e));
     }
  };

  transcribeWebSocket.onmessage = (event) => {
    // *** Parse the incoming event stream message from Transcribe (Task 1.4) ***
    // event.data will be a binary Blob or ArrayBuffer.
    // You need to unmarshall it using EventStreamMarshaller.
    // Implementation (Task 1.4):
    try {
        const blob = event.data;
        // Read blob as ArrayBuffer asynchronously
        const reader = new FileReader();
        reader.onloadend = () => {
            const arrayBuffer = reader.result;
            // Unmarshall the binary message
            const message = eventMarshaller.unmarshall(new Uint8Array(arrayBuffer));

            const messageType = message.headers[':message-type']?.value;
            const eventType = message.headers[':event-type']?.value;

            if (messageType === 'event' && eventType === 'transcript-event') {
                const messageBody = JSON.parse(message.body); // Message body is JSON
                const results = messageBody.Transcript?.Results;

                if (results && results.length > 0) {
                    // Process results. Transcribe can return multiple results/alternatives.
                    // We usually care about the first result and its first alternative.
                    const result = results[0];
                    const alternative = result.Alternatives?.[0];

                    if (alternative && alternative.Transcript && alternative.Transcript.trim().length > 0) {
                        const transcript = alternative.Transcript;
                        const isPartial = result.IsPartial;

                        if (!isPartial) {
                            // Got a final transcript segment
                            console.log("Final Japanese:", transcript);
                            // *** CALL TRANSLATE HERE (Task 1.5) ***
                            translateText(transcript);
                        } else {
                            // Optional: Handle partial results for immediate display (Task 1.4 refinement)
                            // console.log("Partial Japanese:", transcript);
                            // updateSubtitlesInContentScript(transcript, "..."); // Show partial JP, pending translation
                        }
                    }
                }
            } else if (messageType === 'exception') {
                 const messageBody = JSON.parse(message.body);
                 console.error("Transcribe Exception:", messageBody.Message);
                 updateSubtitlesInContentScript("", `[STT出错: ${messageBody.Message}]`);
                 stopProcessing(); // Stop on error
            } else {
                 console.log("Received unknown WebSocket message type:", messageType, eventType);
            }
        };
        // Start reading the blob as ArrayBuffer
        // Need to handle potential errors during reading? FileReader.onerror?
        reader.readAsArrayBuffer(blob);

    } catch (error) {
        console.error("Error processing Transcribe WebSocket message:", error);
        updateSubtitlesInContentScript("", `[处理STT结果出错: ${error.message}]`);
        stopProcessing(); // Stop on error
    }
  };

   transcribeWebSocket.onerror = (event) => {
    console.error("Transcribe WebSocket error:", event);
    const errorMessage = event?.message || 'Unknown WebSocket error';
    // Attempt to get a more specific error message from the event if available
    // WebSocket ErrorEvent doesn't always have a detailed message property
    let detailedError = errorMessage;
    if (event.error) {
        detailedError = event.error.message || detailedError;
    }
    updateSubtitlesInContentScript("", `[STT连接出错: ${detailedError}]`);
    stopProcessing(); // Ensure state is reset
  };

  transcribeWebSocket.onclose = (event) => {
    console.log("Transcribe WebSocket closed:", event.code, event.reason);
    // Check if the closure was unexpected (e.g., not code 1000 or 1001)
    // See https://developer.mozilla.org/en-US/docs/Web/API/CloseEvent/code
    const unexpectedClose = isRunning && event.code !== 1000 && event.code !== 1001;

    if (unexpectedClose) {
       console.error("WebSocket closed unexpectedly, attempting to stop.");
       updateSubtitlesInContentScript("", `[STT连接意外关闭: ${event.code}]`);
       stopProcessing(); // Ensure state is reset
    } else {
       console.log("WebSocket closed gracefully or due to protocol error/going away.");
       // If isRunning is true here, it means stopProcessing was likely called,
       // which initiates the graceful close (code 1000).
       // If it was code 1001 (going away) while running, it's also unexpected.
       if (isRunning && event.code === 1001) {
            console.error("WebSocket closed unexpectedly (going away), attempting to stop.");
            updateSubtitlesInContentScript("", `[STT连接意外关闭: ${event.code}]`);
            stopProcessing();
       }
    }
  };


  // Return the WebSocket instance so the caller knows it was created (even if not fully open yet)
  return transcribeWebSocket;
}

function stopTranscribeWebSocket() {
  if (transcribeWebSocket) {
    console.log("Closing Transcribe WebSocket...");
    // Use code 1000 for normal closure
    transcribeWebSocket.close(1000, "Client stopping");
    transcribeWebSocket = null;
  }
}

// Function to send audio chunks (called by content script)
// The chunk should be an ArrayBuffer containing raw audio data (Int16Array buffer)
// This is Task 1.3. Event Stream marshalling is implemented here.
function sendAudioChunkToTranscribe(chunk) {
    if (!transcribeWebSocket || transcribeWebSocket.readyState !== WebSocket.OPEN) {
        console.error("WebSocket not ready. State:", transcribeWebSocket?.readyState);
        stopProcessing();
        return;
    }

    try {
        // Create the audio event message
        const audioEventMessage = {
            headers: {
                ':message-type': { type: 'string', value: 'event' },
                ':event-type': { type: 'string', value: 'AudioEvent' }
            },
            body: chunk // chunk is already an ArrayBuffer of Int16Array data
        };

        // Marshall the message to binary format
        const binaryMessage = eventMarshaller.marshall(audioEventMessage);
        
        // Send the binary message
        transcribeWebSocket.send(binaryMessage);
    } catch (error) {
        console.error("Error sending audio chunk to Transcribe:", error);
        updateSubtitlesInContentScript("", `[音訊傳送錯誤: ${error.message}]`);
        stopProcessing();
    }
}


// --- AWS Translate Logic (Task 1.5) ---
async function translateText(japaneseText) {
  if (!awsConfig || !translateClient) {
    console.error("AWS config or TranslateClient not loaded for translation.");
    updateSubtitlesInContentScript(japaneseText, "[翻译服务未启动或出错]");
    return;
  }

  if (!japaneseText || japaneseText.trim().length === 0) {
      console.log("No meaningful text to translate.");
      // If Transcribe sends empty or whitespace result, maybe clear subtitles or show only previous?
      // updateSubtitlesInContentScript("", "");
      return;
  }

  console.log("Translating:", japaneseText);

  try {
    // *** Call Translate API (Task 1.5, Requires @aws-sdk/client-translate) ***
    const command = new TranslateTextCommand({
      Text: japaneseText,
      SourceLanguageCode: TRANSLATE_SOURCE_LANGUAGE,
      TargetLanguageCode: TRANSLATE_TARGET_LANGUAGE,
    });
    const response = await translateClient.send(command);
    const chineseText = response.TranslatedText;

    console.log("Translated:", chineseText);

    // Send both original and translated text to content script
    updateSubtitlesInContentScript(japaneseText, chineseText);

  } catch (error) {
    console.error("Error translating text:", error);
    updateSubtitlesInContentScript(japaneseText, `[翻译出错: ${error.message}]`);
  }
}

// Helper to send subtitle update message to the active tab (Same as before)
function updateSubtitlesInContentScript(japaneseText, chineseText) {
    // Send message only to the tab that initiated the process
    if (currentTabId) {
        chrome.tabs.sendMessage(currentTabId, {
          action: "updateSubtitles",
          japanese: japaneseText,
          chinese: chineseText
        }).catch(e => console.error("Error sending updateSubtitles message:", e));
    } else {
        console.warn("No active tab ID to send subtitle update.");
    }
}

// --- Main Control Functions (Same as before, orchestrates capture initiation) ---
async function startProcessing() {
  if (isRunning) {
    console.log("Already running.");
    return { success: false, error: "Already running" };
  }

  console.log("Attempting to start processing...");

  try {
    // Get the active tab first to know where to inject/send messages
    const [tab] = await chrome.tabs.query({active: true, currentWindow: true});
    if (!tab || !tab.id) {
         console.error("No active tab found or tab ID is invalid.");
         return { success: false, error: "No active tab found" };
    }
    currentTabId = tab.id; // Store the tab ID

    // 1. Get AWS Credentials
    awsConfig = await getAwsCredentials();
    console.log("AWS Config loaded successfully.");

    // 2. Initialize AWS Translate Client (Task 1.5)
    // This should be initialized once when starting.
    translateClient = new TranslateClient({ // Requires @aws-sdk/client-translate
       region: awsConfig.region,
       credentials: {
           accessKeyId: awsConfig.accessKeyId,
           secretAccessKey: awsConfig.secretAccessKey,
           // sessionToken: awsConfig.sessionToken, // If using temporary credentials
       }
    });
    console.log("TranslateClient initialized."); // Changed from warn to log


    // 3. Start AWS Transcribe WebSocket connection (Now includes SigV4)
    // This is async and needs to handle connection lifecycle.
    // We start the WS connection early while waiting for audio.
    await startTranscribeWebSocket(awsConfig);
    console.log("Transcribe WebSocket setup initiated.");

    // 4. Inject content script and CSS (if not already there) and signal it to start subtitles
    // This ensures the content script is ready before we ask it to capture.
    // Note: chrome.scripting.executeScript requires 'scripting' permission and host_permissions.
    await chrome.scripting.executeScript({
        target: { tabId: currentTabId },
        files: ['content.js'], // Inject content.js if not already there
    });
     await chrome.scripting.insertCSS({
        target: { tabId: currentTabId },
        files: ['subtitle.css'], // Inject CSS if not already there
    });

    // Send the initial start signal to the content script (primarily for subtitles)
    await chrome.tabs.sendMessage(currentTabId, { action: "startContentScript" });
    console.log("Sent startContentScript signal to content script.");

    // 5. *** NOW, TELL THE CONTENT SCRIPT TO INITIATE TAB CAPTURE ***
    // This will trigger the chrome.tabCapture.capture call and the permission dialog in the content script.
    // The content script will then process audio and send chunks back via 'audioChunk' messages.
    // Requires 'tabCapture' permission in manifest.json.
    await chrome.tabs.sendMessage(currentTabId, { action: "initiateTabCapture" });
    console.log("Sent initiateTabCapture signal to content script.");


    isRunning = true;
    console.log("Processing started successfully.");
    return { success: true };

  } catch (error) {
    console.error("Failed to start processing:", error);
    // Ensure cleanup happens if start fails
    stopProcessing();
    let userError = "Unknown error during start.";
    if (typeof error === 'string') {
        userError = error; // E.g., "Credentials not configured"
    } else if (error instanceof Error) {
        userError = error.message;
    }
    updateSubtitlesInContentScript("", `[启动失败: ${userError}]`); // Show error in subtitles
    return { success: false, error: userError };
  }
}

function stopProcessing() {
  if (!isRunning) {
    console.log("Not running.");
    return { success: false, error: "Not running" };
  }

  console.log("Stopping processing...");

  // 1. Stop AWS Transcribe WebSocket
  stopTranscribeWebSocket();

  // 2. Inform content script to remove subtitles and stop audio capture
  if (currentTabId) {
      chrome.tabs.sendMessage(currentTabId, { action: "stopContentScript" })
        .catch(e => console.error("Error sending stopContentScript message:", e));
      console.log("Sent stop signal to content script.");
  } else {
      console.warn("No active tab ID to send stop signal.");
  }

  // 3. Reset state
  isRunning = false;
  awsConfig = null; // Clear credentials from memory (best effort)
  translateClient = null; // Clear client instance
  currentTabId = null; // Clear the active tab ID
  console.log("Processing stopped.");
  return { success: true };
}

// --- Message Listener from Popup/Content Script (Same as before) ---
chrome.runtime.onMessage.addListener((message, sender, sendResponse) => {
  console.log("Background received message:", message.action, sender.tab ? "from content script in tab " + sender.tab.id : "from popup/other");

  // Handle messages from popup or content script
  if (message.action === "start") {
    // Start the whole process (triggered by popup)
    startProcessing().then(sendResponse);
    return true; // Indicate async response
  } else if (message.action === "stop") {
    // Stop the whole process (triggered by popup)
    sendResponse(stopProcessing());
  } else if (message.action === "getStatus") {
    // Report current running status (used by popup)
    sendResponse({ isRunning: isRunning });
  } else if (message.action === "getStreamId") {
    // Provide a stream ID for tab audio capture
    chrome.tabCapture.getMediaStreamId({ consumerTabId: sender.tab.id }, (streamId) => {
      if (chrome.runtime.lastError || !streamId) {
        console.error("Failed to get stream ID:", chrome.runtime.lastError?.message);
        sendResponse({ streamId: null, error: chrome.runtime.lastError?.message || 'Unable to get stream ID' });
      } else {
        sendResponse({ streamId });
      }
    });
    return true;
  } else if (message.action === "audioChunk") {
    if (isRunning && message.chunk) {
        sendAudioChunkToTranscribe(message.chunk);
    }
  } else if (message.action === "audioCaptureError") {
     // Received error message from content script regarding audio capture
     console.error("Audio capture failed in content script:", message.error);
     // Optionally stop processing or show a notification
     // It's often best to stop the whole process if audio capture fails.
     updateSubtitlesInContentScript("", `[音訊擷取錯誤: ${message.error}]`); // Inform user via subtitles
     stopProcessing(); // Stop the entire process
     sendResponse({ success: true }); // Acknowledge receipt
  } else if (message.action === "audioProcessingStarted") {
      // Optional: Content script confirms Web Audio processing started
      console.log("Audio processing started in content script");
      sendResponse({ success: true });
  }
  // Add other message handlers if needed
});

// --- Initial state check (optional) ---
console.log("Background service worker initialized. State: Idle.");