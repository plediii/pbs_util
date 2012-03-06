
import os

def disable():
    # clear any variables triggering client MPI behavior.
    for key, value in os.environ.iteritems():
        if (key.startswith('OMPI_COMM') or
            key.startswith('OMPI_UNIVERSE') or
            key.startswith('OMPI_MCA') or
            key.startswith('OMPI_FC')):
            os.unsetenv(key)
    
