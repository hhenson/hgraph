export class WebSocketHelper {
    constructor(url, cb = null) {
        this.url = url;
        this.ws = null;
        this.connected = false;
        this.pendingMessages = [];
        this.pendingRequests = new Map();
        this.requestCounter = 0;
        this.connectionPromise = null;
        this.connectionResolve = null;
        this.connectionReject = null;

        this.connectMessages = [];

        this.on_message = cb;
    }

    async connect(timeout = 30000) {
        if (this.connected) return true;
        
        this.connectionPromise = new Promise((resolve, reject) => {
            this.connectionResolve = resolve;
            this.connectionReject = reject;
        });

        this.ws = new WebSocket(this.url);
        
        this.ws.onopen = () => {
            this.connected = true;
            this.connectionResolve(true);
            console.log('Connected to WebSocket');
            for (const message of this.connectMessages) {
                this.send(message);
            }
            const pending = this.pendingMessages;
            this.pendingMessages = [];
            for (const message of pending) {
                this.send(message);
            }
        };

        this.ws.onclose = () => {
            this.connected = false;
            if (this.connectionReject) {
                this.connectionReject(new Error('Connection closed'));
            }
            console.log('Disconnected from WebSocket, attempting to reconnect...');
            setTimeout(() => this.connect().catch(() => {}), 10000);
        };

        this.ws.onmessage = async (event) => {
            try {
                const message = JSON.parse(typeof(event.data) === 'string' || event.data instanceof String ? event.data : await event.data.text());
                if (this.on_message) {
                    this.on_message(message);
                }
            } catch (error) {
                console.error('Error processing message:', error);
            }
        };

        this.ws.onerror = (error) => {
            console.error('WebSocket error:', JSON.stringify(error));
        };

        // Add connection timeout
        const timeoutId = setTimeout(() => {
            if (!this.connected) {
                this.connectionReject(new Error('Connection timeout'));
                this.ws.close();
            }
        }, timeout);

        try {
            await this.connectionPromise;
            clearTimeout(timeoutId);
            return true;
        } catch (error) {
            clearTimeout(timeoutId);
            throw error;
        }
    }

    generateRequestId() {
        return ++this.requestCounter;
    }

    async send(message) {
        if (!this.connected) {
            this.pendingMessages.push(message);
            return;
        }

        const payload = typeof message === "string" || message instanceof String ? `{"message": ${message}}` : JSON.stringify({message});

        try {
            this.ws.send(payload);
        } catch (error) {
            console.error('Error sending message:', error);
            this.pendingMessages.push(message);
        }
    }

    async send_on_connect_message(message) {
        this.connectMessages.push(message);

        if (!this.connected) {
            return;
        }

        const payload = typeof message === "string" || message instanceof String ? `{"message": ${message}}` : JSON.stringify({message});

        try {
            this.ws.send(payload);
        } catch (error) {
            console.error('Error sending message:', error);
        }
    }

    disconnect() {
        if (this.ws) {
            this.ws.close();
        }
    }
}
