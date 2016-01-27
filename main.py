#!/usr/bin/env python

if __name__ == '__main__':
    from tscached import app
    app.debug = True
    app.run(host='0.0.0.0', port=8008)
