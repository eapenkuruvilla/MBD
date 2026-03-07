# Use an appropriate base image (e.g., Node.js, Python, etc.)
FROM node:18-alpine
RUN apk add --no-cache bash libgcc libstdc++ ripgrep python3
ENV USE_BUILTIN_RIPGREP=0
ENV SHELL=/bin/bash

# Set the working directory inside the container
WORKDIR /MBD

# Install the Claude Code CLI globally
RUN npm install -g @anthropic-ai/claude-code
RUN pip install pytest

# Set the default command to start an interactive session
CMD ["claude"]

