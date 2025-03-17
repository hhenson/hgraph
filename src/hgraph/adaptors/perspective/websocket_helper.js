export class WebSocketHelper {
    constructor(url) {
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
            for (const message of this.pendingMessages) {
                this.send(message);
            }
            this.pendingMessages = [];
        };

        this.ws.onclose = () => {
            this.connected = false;
            if (this.connectionReject) {
                this.connectionReject(new Error('Connection closed'));
            }
            console.log('Disconnected from WebSocket, attempting to reconnect...');
            setTimeout(() => this.connect(), 3000);
        };

        this.ws.onmessage = (event) => {
            try {
                const response = JSON.parse(event.data);
                if (response.request_id && this.pendingRequests.has(response.request_id)) {
                    const { resolve, reject } = this.pendingRequests.get(response.request_id);
                    this.pendingRequests.delete(response.request_id);
                    resolve(response.message);
                }
            } catch (error) {
                console.error('Error processing message:', error);
            }
        };

        this.ws.onerror = (error) => {
            console.error('WebSocket error:', error);
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

        const payload = {
            message
        };

        try {
            this.ws.send(JSON.stringify(payload));
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

        const payload = {
            message
        };

        try {
            this.ws.send(JSON.stringify(payload));
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
