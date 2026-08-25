//go:build amd64 && linux

package telegram

import (
	"encoding/binary"
	"math"
)

const (
	// ntgcalls NTG_EXTERNAL: PCM16 48kHz mono.
	ntgSampleRate   = 48000
	ntgChannels     = 1
	ntgFrameSamples = 480                 // 10ms @ 48kHz mono
	ntgFrameBytes   = ntgFrameSamples * 2 // PCM16 = 2 байта/сэмпл → 960 байт = 10ms

	// OpenAI Realtime API: PCM16 24kHz mono
	oaiSampleRate = 24000
)

// pcm16Resample выполняет линейную интерполяцию PCM16 mono.
// src — входные байты (little-endian int16),
// fromHz, toHz — частоты дискретизации.
func pcm16Resample(src []byte, fromHz, toHz int) []byte {
	if fromHz == toHz {
		return src
	}
	srcSamples := len(src) / 2
	if srcSamples == 0 {
		return nil
	}
	dstSamples := int(math.Round(float64(srcSamples) * float64(toHz) / float64(fromHz)))
	if dstSamples == 0 {
		return nil
	}
	dst := make([]byte, dstSamples*2)

	ratio := float64(srcSamples-1) / float64(dstSamples-1)

	for i := 0; i < dstSamples; i++ {
		pos := float64(i) * ratio
		idx := int(pos)
		frac := pos - float64(idx)

		s0 := int16(binary.LittleEndian.Uint16(src[idx*2:]))
		var s1 int16
		if idx+1 < srcSamples {
			s1 = int16(binary.LittleEndian.Uint16(src[(idx+1)*2:]))
		} else {
			s1 = s0
		}
		val := int16(float64(s0)*(1-frac) + float64(s1)*frac)
		binary.LittleEndian.PutUint16(dst[i*2:], uint16(val))
	}
	return dst
}

type audioFramer struct {
	buf       []byte
	frameSize int // в байтах
}

func newAudioFramer(frameBytes int) *audioFramer {
	return &audioFramer{frameSize: frameBytes}
}

// Push добавляет данные в буфер. Возвращает готовые фреймы.
func (f *audioFramer) Push(data []byte) [][]byte {
	f.buf = append(f.buf, data...)
	var frames [][]byte
	for len(f.buf) >= f.frameSize {
		frame := make([]byte, f.frameSize)
		copy(frame, f.buf[:f.frameSize])
		f.buf = f.buf[f.frameSize:]
		frames = append(frames, frame)
	}
	return frames
}

// Flush возвращает остаток буфера с padding до frameSize (тишина).
func (f *audioFramer) Flush() []byte {
	if len(f.buf) == 0 {
		return nil
	}
	frame := make([]byte, f.frameSize)
	copy(frame, f.buf)
	f.buf = nil
	return frame
}

// attenuatePCM16 умножает каждый PCM16 сэмпл на gain [0.0..1.0].
// Используется для подавления акустического эха пока модель воспроизводит аудио:
// аттенюированный сигнал не триггерит VAD но сохраняет возможность перебить модель
// реальным голосом (который значительно громче эха).
func attenuatePCM16(pcm []byte, gain float64) []byte {
	if len(pcm) < 2 {
		return pcm
	}
	out := make([]byte, len(pcm))
	for i := 0; i+1 < len(pcm); i += 2 {
		s := int16(binary.LittleEndian.Uint16(pcm[i:]))
		s = int16(float64(s) * gain)
		binary.LittleEndian.PutUint16(out[i:], uint16(s))
	}
	return out
}
